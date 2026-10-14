# Multi-Threaded TCP Chat Server

Real-time chat server built with Python sockets and threads. No third-party dependencies (Python 3.8+).

## Files

| File | Purpose |
|---|---|
| `server.py` | Multi-threaded TCP chat server |
| `client.py` | Interactive terminal client |
| `stress_test.py` | Concurrent load generator and delivery verifier |

## Run

```bash
python server.py --port 5000
python client.py --port 5000
python client.py --port 5000
```

Server options: `--host`, `--port`, `--max-clients`, `--verbose`

## Chat commands

| Command | Description |
|---|---|
| `/help` | Show commands |
| `/users` | List online users |
| `/whisper <nick> <text>` | Private message |
| `/nick <new_nick>` | Change nickname |
| `/quit` | Disconnect gracefully |

## Stress test

```bash
python stress_test.py --port 5000 --clients 200 --messages 20
```

It connects N clients, waits until all are ready, sends messages simultaneously, waits for delivery to drain, then reports connection failures, delivery ratio, throughput, and whether the server still accepts new connections.

Sample result (200 clients, 4000 messages, 796,000 expected deliveries): 100% delivered, 0 connection failures, server responsive.

## Design

- **Thread per connection**: the main thread accepts connections and spawns a handler thread for each client.
- **Shared state**: the nickname registry is a dictionary guarded by a single `threading.Lock`; broadcast takes a snapshot of recipients under the lock and delivers outside it.
- **Per-client outbox**: each client has a bounded queue and a dedicated writer thread, so one slow client never blocks broadcasts to others. Clients whose queue fills up are disconnected.
- **Framing**: newline-delimited messages with a per-connection receive buffer, so partial and coalesced TCP reads are handled correctly. Lines over 2 KB are rejected.
- **Lifecycle**: nickname registration (validation, uniqueness, retry limit), join/leave announcements, and cleanup in `finally` blocks so every exit path releases resources.
- **Graceful shutdown**: SIGINT/SIGTERM stops the accept loop, notifies connected clients, and closes all sockets.
