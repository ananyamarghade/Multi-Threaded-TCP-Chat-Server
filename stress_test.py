import argparse
import socket
import threading
import time

MARKER = b"stress-msg"


class StressRun:
    def __init__(self, args):
        self.args = args
        self.go = threading.Event()
        self.finished = threading.Event()
        self.done_sending = 0
        self.lock = threading.Lock()
        self.connected = 0
        self.failed = 0
        self.sent = [0] * args.clients
        self.received = [0] * args.clients

    def reader(self, idx, sock):
        buffer = b""
        try:
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                buffer += data
                *lines, buffer = buffer.split(b"\n")
                self.received[idx] += sum(1 for line in lines if MARKER in line)
        except OSError:
            pass

    def client(self, idx):
        args = self.args
        try:
            sock = socket.create_connection((args.host, args.port), timeout=10)
            sock.settimeout(None)
            sock.sendall(f"user{idx}\n".encode())
        except OSError:
            with self.lock:
                self.failed += 1
            return
        with self.lock:
            self.connected += 1
        reader = threading.Thread(target=self.reader, args=(idx, sock), daemon=True)
        reader.start()
        try:
            self.go.wait(timeout=120)
            for n in range(args.messages):
                sock.sendall(f"{MARKER.decode()} {idx} {n}\n".encode())
                self.sent[idx] += 1
                if args.delay:
                    time.sleep(args.delay)
            with self.lock:
                self.done_sending += 1
            self.finished.wait(timeout=300)
            sock.sendall(b"/quit\n")
        except OSError:
            pass
        reader.join(timeout=5)
        try:
            sock.close()
        except OSError:
            pass

    def run(self):
        args = self.args
        threads = [
            threading.Thread(target=self.client, args=(i,)) for i in range(args.clients)
        ]
        for thread in threads:
            thread.start()
            if args.ramp:
                time.sleep(args.ramp)
        deadline = time.time() + 60
        while time.time() < deadline:
            with self.lock:
                if self.connected + self.failed >= args.clients:
                    break
            time.sleep(0.05)
        time.sleep(0.5)
        started = time.time()
        self.go.set()
        while True:
            with self.lock:
                if self.done_sending >= self.connected:
                    break
            time.sleep(0.02)
        last = -1
        stable_since = time.time()
        drain_deadline = time.time() + 120
        while time.time() < drain_deadline:
            current = sum(self.received)
            if current != last:
                last = current
                stable_since = time.time()
            elif time.time() - stable_since >= self.args.settle:
                break
            time.sleep(0.1)
        elapsed = stable_since - started
        self.finished.set()
        for thread in threads:
            thread.join()
        return elapsed


def probe(host, port):
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.settimeout(5)
        data = sock.recv(256)
        sock.close()
        return b"nickname" in data.lower()
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser(description="Stress test for the chat server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument("--delay", type=float, default=0.01)
    parser.add_argument("--ramp", type=float, default=0.002)
    parser.add_argument("--settle", type=float, default=2.0)
    args = parser.parse_args()

    run = StressRun(args)
    elapsed = run.run()

    total_sent = sum(run.sent)
    total_received = sum(run.received)
    expected = total_sent * max(run.connected - 1, 0)
    ratio = (total_received / expected * 100) if expected else 0.0
    throughput = total_received / elapsed if elapsed else 0.0

    print(f"clients requested : {args.clients}")
    print(f"clients connected : {run.connected}")
    print(f"connect failures  : {run.failed}")
    print(f"messages sent     : {total_sent}")
    print(f"deliveries        : {total_received} / {expected} ({ratio:.2f}%)")
    print(f"elapsed (s)       : {elapsed:.2f}")
    print(f"deliveries per sec: {throughput:.0f}")
    print(f"server responsive : {'yes' if probe(args.host, args.port) else 'no'}")


if __name__ == "__main__":
    main()
