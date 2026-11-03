"""Tests for the peer server: talk to it with raw sockets, like another peer would."""

import hashlib
import os
import socket

import pytest

from chunkswarm.manifest import MIN_CHUNK_SIZE
from chunkswarm.peer_server import PeerServer
from chunkswarm.protocol import MsgType, recv_msg, send_msg

CS = MIN_CHUNK_SIZE  # 64 KiB chunks keep the tests fast


@pytest.fixture
def server(tmp_path):
    share = tmp_path / "share"
    share.mkdir()
    data = os.urandom(CS * 3 + 500)          # 4 chunks, the last one is 500 bytes
    (share / "movie.bin").write_bytes(data)
    srv = PeerServer(share, chunk_size=CS, max_connections=2, idle_timeout=0.5).start()
    manifest = srv.share(share / "movie.bin")
    yield srv, manifest, data
    srv.stop()


def ask(srv, msg_type, header=None):
    with socket.create_connection(("127.0.0.1", srv.port), timeout=2) as s:
        send_msg(s, msg_type, header)
        return recv_msg(s)


# ------------------------------------------------------------- normal use
def test_ping(server):
    srv, _, _ = server
    assert ask(srv, MsgType.PING)[0] is MsgType.PONG


def test_get_manifest(server):
    srv, m, _ = server
    t, h, _ = ask(srv, MsgType.GET_MANIFEST, {"file_id": m.file_id})
    assert t is MsgType.MANIFEST and h["manifest"] == m.to_dict()


def test_get_every_chunk_and_rebuild_the_file(server):
    srv, m, data = server
    rebuilt = b""
    with socket.create_connection(("127.0.0.1", srv.port), timeout=2) as s:
        for i in range(m.num_chunks):                 # one connection, many requests
            send_msg(s, MsgType.GET_CHUNK, {"file_id": m.file_id, "index": i})
            t, h, payload = recv_msg(s)
            assert t is MsgType.CHUNK_DATA and h["index"] == i
            assert hashlib.sha256(payload).hexdigest() == m.chunk_hashes[i]
            rebuilt += payload
    assert rebuilt == data
    assert srv.stats["chunks_served"] == 4


# ------------------------------------------------------------- attacks
@pytest.mark.parametrize("header", [
    {"file_id": "../../etc/passwd", "index": 0},
    {"file_id": "/etc/passwd", "index": 0},
    {"file_id": "f" * 64, "index": 0},           # looks valid, but not shared here
    {"index": 0},                                # no file_id at all
])
def test_unknown_or_path_like_file_ids_rejected(server, header):
    srv, _, _ = server
    t, h, payload = ask(srv, MsgType.GET_CHUNK, header)
    assert t is MsgType.ERROR and h["code"] == "NOT_FOUND" and not payload


@pytest.mark.parametrize("index", [-1, 4, 999, "0", 1.0, None])
def test_bad_chunk_index_rejected(server, index):
    srv, m, _ = server
    t, h, _ = ask(srv, MsgType.GET_CHUNK, {"file_id": m.file_id, "index": index})
    assert t is MsgType.ERROR and h["code"] == "BAD_INDEX"


def test_cannot_share_files_outside_the_share_folder(tmp_path):
    share = tmp_path / "share"
    share.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_text("my passwords")
    try:
        (share / "sneaky_link.txt").symlink_to(secret)   # a shortcut pointing outside
    except OSError:
        pytest.skip("this OS needs admin rights to create symlinks")
    srv = PeerServer(share, chunk_size=CS)
    with pytest.raises(PermissionError):
        srv.share(secret)
    with pytest.raises(PermissionError):
        srv.share(share / "sneaky_link.txt")
    assert srv.share_all() == []


def test_garbage_bytes_get_an_error_and_disconnect(server):
    srv, _, _ = server
    with socket.create_connection(("127.0.0.1", srv.port), timeout=2) as s:
        s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")   # someone speaking HTTP to us
        t, h, _ = recv_msg(s)
        assert t is MsgType.ERROR and h["code"] == "PROTOCOL"


def test_clients_cannot_upload_bytes(server):
    srv, m, _ = server
    with socket.create_connection(("127.0.0.1", srv.port), timeout=2) as s:
        send_msg(s, MsgType.GET_CHUNK, {"file_id": m.file_id, "index": 0}, b"x" * 1000)
        t, h, _ = recv_msg(s)
        assert t is MsgType.ERROR and h["code"] == "PROTOCOL"


def test_connection_limit(server):
    srv, _, _ = server                                 # max_connections=2
    held = [socket.create_connection(("127.0.0.1", srv.port), timeout=2) for _ in range(2)]
    for h in held:
        send_msg(h, MsgType.PING)
        assert recv_msg(h)[0] is MsgType.PONG
    with socket.create_connection(("127.0.0.1", srv.port), timeout=2) as third:
        t, h, _ = recv_msg(third)
        assert t is MsgType.ERROR and h["code"] == "BUSY"
    for h in held:
        h.close()


def test_silent_connections_are_dropped(server):
    srv, _, _ = server                                 # idle_timeout=0.5
    with socket.create_connection(("127.0.0.1", srv.port), timeout=3) as s:
        assert s.recv(1) == b""                        # server hung up on us