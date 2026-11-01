import socket, threading
from chunkswarm.protocol import recv_exact

a, b = socket.socketpair()      # two sockets already connected to each other

def send_in_pieces():
    for piece in [b"hel", b"lo w", b"orld"]:
        a.sendall(piece)

threading.Thread(target=send_in_pieces).start()
print(recv_exact(b, 11))        # ask for exactly 11 bytes