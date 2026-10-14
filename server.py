import argparse
import logging
import queue
import re
import signal
import socket
import threading
import time

NICK_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,20}$")
MAX_LINE_BYTES = 2048
OUTBOX_SIZE = 10000
MAX_NICK_ATTEMPTS = 5

HELP_TEXT = (
    "Commands:\n"
    "  /help                    show this help\n"
    "  /users                   list online users\n"
    "  /whisper <nick> <text>   private message\n"
    "  /nick <new_nick>         change nickname\n"
    "  /quit                    disconnect"
)


class Client:
    def __init__(self, conn, addr):
        self.conn = conn
        self.addr = addr
        self.nick = None
        self.buffer = b""
        self.outbox = queue.Queue(maxsize=OUTBOX_SIZE)
        self.writer = threading.Thread(target=self.writer_loop, daemon=True)

    def enqueue(self, text):
        try:
            self.outbox.put_nowait(text)
            return True
        except queue.Full:
            return False

    def writer_loop(self):
        while True:
            item = self.outbox.get()
            if item is None:
                break
            try:
                self.conn.sendall((item + "\n").encode("utf-8"))
            except OSError:
                break
        self.close()

    def readline(self):
        while b"\n" not in self.buffer:
            if len(self.buffer) > MAX_LINE_BYTES:
                raise ValueError("line too long")
            chunk = self.conn.recv(4096)
            if not chunk:
                return None
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        return line.decode("utf-8", errors="replace").strip()

    def finish(self):
        try:
            self.outbox.put(None, timeout=1)
            self.writer.join(timeout=2)
        except queue.Full:
            pass
        self.close()

    def close(self):
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.conn.close()
        except OSError:
            pass


class ChatServer:
    def __init__(self, host, port, max_clients):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.clients = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.listener = None

    def serve_forever(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.host, self.port))
        self.listener.listen(256)
        self.listener.settimeout(1.0)
        logging.info("listening on %s:%d", self.host, self.port)
        try:
            while not self.stop_event.is_set():
                try:
                    conn, addr = self.listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client = Client(conn, addr)
                thread = threading.Thread(
                    target=self.handle_client, args=(client,), daemon=True
                )
                thread.start()
        finally:
            self.shutdown()

    def shutdown(self):
        self.stop_event.set()
        if self.listener is not None:
            try:
                self.listener.close()
            except OSError:
                pass
        with self.lock:
            remaining = list(self.clients.values())
        for client in remaining:
            client.enqueue("* server is shutting down")
        time.sleep(0.5)
        for client in remaining:
            client.close()
        logging.info("server stopped")

    def handle_client(self, client):
        client.writer.start()
        logging.info("connection from %s:%d", *client.addr)
        try:
            if not self.register(client):
                return
            self.broadcast(f"* {client.nick} joined the chat", exclude=client)
            client.enqueue(
                f"Welcome {client.nick}! {self.online_count()} user(s) online. "
                "Type /help for commands."
            )
            while not self.stop_event.is_set():
                line = client.readline()
                if line is None:
                    break
                if not line:
                    continue
                if line.startswith("/"):
                    if not self.handle_command(client, line):
                        break
                else:
                    self.broadcast(f"[{client.nick}] {line}", exclude=client)
        except (OSError, ValueError) as exc:
            logging.info("client %s error: %s", client.nick or client.addr, exc)
        finally:
            self.remove(client)
            client.finish()
            logging.info(
                "disconnected %s (%d online)", client.nick or client.addr, self.online_count()
            )

    def register(self, client):
        client.enqueue("Enter nickname:")
        for _ in range(MAX_NICK_ATTEMPTS):
            line = client.readline()
            if line is None:
                return False
            if not NICK_PATTERN.match(line):
                client.enqueue("Invalid nickname. Use 1-20 letters, digits or underscores.")
                continue
            with self.lock:
                if len(self.clients) >= self.max_clients:
                    client.enqueue("Server is full. Try again later.")
                    return False
                if line.lower() in self.clients:
                    taken = True
                else:
                    taken = False
                    client.nick = line
                    self.clients[line.lower()] = client
            if not taken:
                logging.info("registered %s (%d online)", line, self.online_count())
                return True
            client.enqueue("Nickname already taken. Try another.")
        client.enqueue("Too many invalid attempts.")
        return False

    def handle_command(self, client, line):
        parts = line.split(None, 2)
        command = parts[0].lower()
        if command == "/quit":
            client.enqueue("Goodbye!")
            return False
        if command == "/help":
            client.enqueue(HELP_TEXT)
        elif command == "/users":
            with self.lock:
                names = sorted(c.nick for c in self.clients.values())
            client.enqueue(f"Online ({len(names)}): " + ", ".join(names))
        elif command == "/whisper":
            self.whisper(client, parts)
        elif command == "/nick":
            self.change_nick(client, parts)
        else:
            client.enqueue(f"Unknown command: {command}. Type /help.")
        return True

    def whisper(self, client, parts):
        if len(parts) < 3:
            client.enqueue("Usage: /whisper <nick> <text>")
            return
        with self.lock:
            target = self.clients.get(parts[1].lower())
        if target is None:
            client.enqueue(f"No such user: {parts[1]}")
            return
        target.enqueue(f"[whisper from {client.nick}] {parts[2]}")
        client.enqueue(f"[whisper to {target.nick}] {parts[2]}")

    def change_nick(self, client, parts):
        if len(parts) < 2 or not NICK_PATTERN.match(parts[1]):
            client.enqueue("Usage: /nick <new_nick> (1-20 letters, digits or underscores)")
            return
        new_nick = parts[1]
        with self.lock:
            existing = self.clients.get(new_nick.lower())
            if existing is not None and existing is not client:
                result = "taken"
            else:
                old_nick = client.nick
                del self.clients[old_nick.lower()]
                client.nick = new_nick
                self.clients[new_nick.lower()] = client
                result = "ok"
        if result == "taken":
            client.enqueue("Nickname already taken.")
        else:
            self.broadcast(f"* {old_nick} is now known as {new_nick}")

    def broadcast(self, text, exclude=None):
        with self.lock:
            targets = [c for c in self.clients.values() if c is not exclude]
        for target in targets:
            if not target.enqueue(text):
                logging.warning("dropping slow client %s", target.nick)
                target.close()

    def remove(self, client):
        removed = False
        with self.lock:
            if client.nick and self.clients.get(client.nick.lower()) is client:
                del self.clients[client.nick.lower()]
                removed = True
        if removed:
            self.broadcast(f"* {client.nick} left the chat")

    def online_count(self):
        with self.lock:
            return len(self.clients)


def main():
    parser = argparse.ArgumentParser(description="Multi-threaded TCP chat server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--max-clients", type=int, default=1000)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )

    server = ChatServer(args.host, args.port, args.max_clients)
    signal.signal(signal.SIGINT, lambda *_: server.stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: server.stop_event.set())
    server.serve_forever()


if __name__ == "__main__":
    main()
