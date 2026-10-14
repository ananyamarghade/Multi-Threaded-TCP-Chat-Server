import argparse
import socket
import sys
import threading


def receive_loop(sock, done):
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                break
            sys.stdout.write(data.decode("utf-8", errors="replace"))
            sys.stdout.flush()
    except OSError:
        pass
    finally:
        done.set()


def main():
    parser = argparse.ArgumentParser(description="TCP chat client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    try:
        sock = socket.create_connection((args.host, args.port), timeout=10)
    except OSError as exc:
        print(f"Could not connect to {args.host}:{args.port}: {exc}")
        sys.exit(1)
    sock.settimeout(None)

    done = threading.Event()
    receiver = threading.Thread(target=receive_loop, args=(sock, done), daemon=True)
    receiver.start()

    try:
        while not done.is_set():
            line = input()
            sock.sendall((line + "\n").encode("utf-8"))
            if line.strip().lower() == "/quit":
                done.wait(2)
                break
    except (EOFError, KeyboardInterrupt, OSError):
        pass
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
        receiver.join(timeout=1)


if __name__ == "__main__":
    main()
