"""Step 2 warm-up: a TCP echo server that shows HOW data really arrives.

Run:  python playground/echo_server.py
Stop: Ctrl+C
"""

import socket

HOST = "127.0.0.1"   # only this computer can connect (localhost)
PORT = 5000          # the "door number" clients knock on

# 1. Create a TCP socket.  AF_INET = IPv4,  SOCK_STREAM = TCP
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# 2. Allow restarting the server immediately on the same port.
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# 3. Claim the address + port, then start listening for clients.
server.bind((HOST, PORT))
server.listen()
print(f"Server listening on {HOST}:{PORT} ... (Ctrl+C to stop)")

try:
    while True:
        # 4. Wait until a client connects. accept() returns a NEW socket just
        #    for this client, plus the client's (IP, port).
        conn, addr = server.accept()
        print(f"\n--- client connected from {addr[0]}:{addr[1]} ---")

        total = 0
        recv_calls = 0
        with conn:
            while True:
                # 5. Ask for UP TO 65536 bytes. TCP may give us fewer.
                data = conn.recv(65536)
                if not data:          # b"" means the client closed its side
                    break
                recv_calls += 1
                total += len(data)
                preview = data[:20]
                print(f"recv #{recv_calls}: got {len(data):>6} bytes   {preview!r}")
                # 6. Echo the same bytes back.
                conn.sendall(data)
        print(f"--- client left: {total} bytes in {recv_calls} recv() calls ---")
except KeyboardInterrupt:
    print("\nServer stopped.")
finally:
    server.close()