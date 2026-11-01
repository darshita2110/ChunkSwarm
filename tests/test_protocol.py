import socket
import struct
import threading

import pytest

from chunkswarm.protocol import (MAX_HEADER_SIZE, PREFIX_FORMAT, PROTOCOL_VERSION, ConnectionClosed,
                          MsgType, ProtocolError, recv_msg, send_msg)


@pytest.fixture
def pair():
    a, b = socket.socketpair()
    a.settimeout(2)
    b.settimeout(2)
    yield a, b
    a.close()
    b.close()


def test_roundtrip_with_binary_payload(pair):
    a, b = pair
    payload = bytes(range(256)) * 1000  # bigger than the socket buffer: send concurrently
    threading.Thread(target=send_msg, args=(a, MsgType.CHUNK_DATA, {"index": 3}, payload)).start()
    t, h, p = recv_msg(b)
    assert t is MsgType.CHUNK_DATA and h == {"index": 3} and bytes(p) == payload


def test_message_split_across_many_tiny_writes(pair):
    """TCP may deliver a frame in pieces; the reader must reassemble it."""
    a, b = pair
    probe_a, probe_b = socket.socketpair()
    send_msg(probe_a, MsgType.GET_CHUNK, {"file_id": "x", "index": 1}, b"")
    raw = probe_b.recv(4096)
    probe_a.close(), probe_b.close()

    def dribble():
        for byte in raw:
            a.sendall(bytes([byte]))
    threading.Thread(target=dribble).start()
    t, h, _ = recv_msg(b)
    assert t is MsgType.GET_CHUNK and h["index"] == 1


def test_two_messages_in_one_write(pair):
    a, b = pair
    send_msg(a, MsgType.PING)
    send_msg(a, MsgType.PONG)
    assert recv_msg(b)[0] is MsgType.PING
    assert recv_msg(b)[0] is MsgType.PONG


def test_clean_close_between_messages(pair):
    a, b = pair
    a.close()
    with pytest.raises(ConnectionClosed):
        recv_msg(b)


def test_truncated_message_is_an_error_not_a_clean_close(pair):
    a, b = pair
    a.sendall(struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, MsgType.PING, 10, 0) + b"{}")
    a.close()
    with pytest.raises(ConnectionError) as ei:
        recv_msg(b)
    assert not isinstance(ei.value, ConnectionClosed)


@pytest.mark.parametrize("prefix,why", [
    (struct.pack(PREFIX_FORMAT, 99, MsgType.PING, 0, 0), "version"),
    (struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, 250, 0, 0), "unknown message type"),
    (struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, MsgType.PING, MAX_HEADER_SIZE + 1, 0), "header too large"),
    (struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, MsgType.PING, 0, 2**40), "payload too large"),
])
def test_bad_prefix_rejected_before_allocating(pair, prefix, why):
    a, b = pair
    a.sendall(prefix)
    with pytest.raises(ProtocolError, match=why):
        recv_msg(b)


def test_header_must_be_json_object(pair):
    a, b = pair
    body = b"[1,2]"
    a.sendall(struct.pack(PREFIX_FORMAT, PROTOCOL_VERSION, MsgType.PING, len(body), 0) + body)
    with pytest.raises(ProtocolError):
        recv_msg(b)


def test_caller_payload_limit(pair):
    a, b = pair
    send_msg(a, MsgType.CHUNK_DATA, {}, b"x" * 100)
    with pytest.raises(ProtocolError):
        recv_msg(b, max_payload=10)