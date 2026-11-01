import socket, struct
from chunkswarm.protocol import send_msg, recv_exact, MsgType, PREFIX_FORMAT, PREFIX_SIZE

a, b = socket.socketpair()
send_msg(a, MsgType.GET_CHUNK, {"index": 3})

prefix = recv_exact(b, PREFIX_SIZE)
print("raw prefix:", prefix.hex(" "))

version, msg_type, header_len, payload_len = struct.unpack(PREFIX_FORMAT, prefix)
print("version:", version, "type:", msg_type, "header_len:", header_len, "payload_len:", payload_len)
print("header:", recv_exact(b, header_len))