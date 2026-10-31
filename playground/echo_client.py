"""Step 2 warm-up: a TCP client with three experiments.

Run one of:
    python playground/echo_client.py small   # send one short message
    python playground/echo_client.py two     # send two messages back to back
    python playground/echo_client.py big     # send 5 MB in ONE sendall()
"""

import socket
import sys
import threading

HOST, PORT = "127.0.0.1", 5000
mode = sys.argv[1] if len(sys.argv) > 1 else "small"

# Connect to the server. This is where the TCP three-way handshake happens.
sock = socket.create_connection((HOST, PORT))
print(f"Connected. My address is {sock.getsockname()}, server is {sock.getpeername()}")

# Read the echo in a background thread, so sending and receiving can happen
# at the same time (otherwise a 5 MB send could get stuck waiting).
received = bytearray()

def reader():
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        received.extend(chunk)

t = threading.Thread(target=reader)
t.start()

if mode == "small":
    sock.sendall(b"hello server")
elif mode == "two":
    sock.sendall(b"hello")    # two separate sends...
    sock.sendall(b"world")    # ...with no pause between them
elif mode == "big":
    payload = b"x" * (5 * 1024 * 1024)   # 5 MB
    sock.sendall(payload)                # ONE call
    print(f"Sent {len(payload)} bytes with a single sendall()")
else:
    sys.exit("mode must be small, two or big")

# Tell the server "I'm done sending" (but keep reading the echo).
sock.shutdown(socket.SHUT_WR)
t.join()
sock.close()

print(f"Got back {len(received)} bytes. First 30: {bytes(received[:30])!r}")