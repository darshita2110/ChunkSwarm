r"""ChunkSwarm wire protocol: how two peers send whole messages over TCP.

Why this file exists
--------------------
TCP is a byte stream (you saw this in the playground): one send can arrive
as many recv()s, and two sends can arrive glued together. So every message
we send starts with a fixed-size PREFIX that says how long the rest is.

One message ("frame") on the wire
---------------------------------
    +---------+----------+------------+-------------+-------------+-------------+
    | version | msg_type | header_len | payload_len |   header    |   payload   |
    | 1 byte  | 1 byte   | 4 bytes    | 8 bytes     | JSON bytes  | raw bytes   |
    +---------+----------+------------+-------------+-------------+-------------+
    \______________ prefix: always 14 bytes _______/

- header  = small JSON object with control info, e.g. {"file_id": "...", "index": 3}
- payload = raw file bytes (only CHUNK_DATA has one), never Base64
- all numbers are big-endian ("network byte order")

YOUR TASK: fill in every TODO. Run `pytest tests/test_protocol.py -v` often.
"""

from __future__ import annotations

import json
import socket
import struct
from enum import IntEnum

# ----------------------------------------------------------------- constants
PROTOCOL_VERSION = 1

# struct format for the 14-byte prefix:
#   !  = network byte order (big-endian)
#   B  = unsigned 1-byte int   (version)
#   B  = unsigned 1-byte int   (msg_type)
#   I  = unsigned 4-byte int   (header_len)
#   Q  = unsigned 8-byte int   (payload_len)
PREFIX_FORMAT = "!BBIQ"
PREFIX_SIZE = struct.calcsize(PREFIX_FORMAT)  # = 14

MAX_HEADER_SIZE = 16 * 1024          # 16 KiB is plenty for JSON control info
MAX_PAYLOAD_SIZE = 4 * 1024 * 1024   # 4 MiB: bigger than any chunk we use


class MsgType(IntEnum):
    PING = 1          # "are you alive?"
    PONG = 2          # "yes"
    GET_MANIFEST = 3  # "describe file X"
    MANIFEST = 4      # the description
    GET_CHUNK = 5     # "send me chunk N of file X"
    CHUNK_DATA = 6    # the chunk bytes (in the payload)
    ERROR = 7         # something was wrong with the request


class ProtocolError(Exception):
    """The other side broke the rules (bad version, too big, not JSON...)."""


class ConnectionClosed(ConnectionError):
    """The other side closed the connection cleanly, BETWEEN messages."""


# ------------------------------------------------------------------- reading
def recv_exact(sock: socket.socket, n: int, *, eof_ok: bool = False) -> bytearray:
    """Read EXACTLY n bytes from sock, however many recv() calls that takes.

    - If the other side closes before ANY byte arrived and eof_ok is True,
      raise ConnectionClosed (a normal goodbye between messages).
    - If it closes at any other moment, raise ConnectionError
      (the message was cut in half).
    """
    # TODO 1:
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        r = sock.recv_into(view[got:], n - got)
        if r == 0:
            if got == 0 and eof_ok:
                raise ConnectionClosed("connection closed by peer")
            raise ConnectionError(f"connection lost mid-message ({got}/{n} bytes)")
        got += r
    return buf


def recv_msg(sock: socket.socket, *, max_payload: int = MAX_PAYLOAD_SIZE
             ) -> tuple[MsgType, dict, bytearray]:
    """Read one whole message. Returns (msg_type, header_dict, payload).

    SECURITY RULE: check the prefix BEFORE reading the rest. If an attacker
    says payload_len = 1 terabyte, we must refuse, not try to allocate it.
    """
    # TODO 2: read the prefix
    prefix = recv_exact(sock, PREFIX_SIZE, eof_ok=True)
    version, raw_type, header_len, payload_len = struct.unpack(PREFIX_FORMAT, prefix)

    # TODO 3: validate, raising ProtocolError with these messages
    if version != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported protocol version {version}")
    try:
        msg_type = MsgType(raw_type)
    except ValueError:
        raise ProtocolError(f"unknown message type {raw_type}") from None
    if header_len > MAX_HEADER_SIZE:
        raise ProtocolError("header too large")
    if payload_len > max_payload:
        raise ProtocolError("payload too large")

    # TODO 4: read and parse the header
    if header_len:
        try:
            header = json.loads(recv_exact(sock, header_len).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ProtocolError("header is not valid JSON") from None
        if not isinstance(header, dict):
            raise ProtocolError("header must be a JSON object")


    # TODO 5: read the payload (payload_len bytes, or bytearray() if 0)
    payload = recv_exact(sock, payload_len) if payload_len else bytearray()
    return msg_type, header, payload



# ------------------------------------------------------------------- writing
def send_msg(sock: socket.socket, msg_type: MsgType, header: dict | None = None,
             payload: bytes | bytearray | memoryview = b"") -> None:
    """Send one whole message."""
    # TODO 6:
    header_bytes = json.dumps(header or {}, separators=(",", ":")).encode("utf-8")
    if len(header_bytes) > MAX_HEADER_SIZE:
        raise ProtocolError("header too large")
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ProtocolError("payload too large")
    prefix = struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, int(msg_type),
                         len(header_bytes), len(payload))
    sock.sendall(prefix + header_bytes)
    if payload:
        sock.sendall(payload)


def send_error(sock: socket.socket, code: str, message: str) -> None:
    """Best-effort ERROR reply. Never raises (the connection may already be dead)."""
    try:
        send_msg(sock, MsgType.ERROR, {"code": code, "message": message})
    except OSError:
        pass