"""Tests for the downloader: real peer servers (some honest, some evil) on localhost."""

import hashlib
import os
import socket
import threading

import pytest

from chunkswarm.downloader import DownloadError, IntegrityError, Source, download
from chunkswarm.manifest import MIN_CHUNK_SIZE
from chunkswarm.peer_server import PeerServer

CS = MIN_CHUNK_SIZE  # 64 KiB chunks keep the tests fast


class CorruptingServer(PeerServer):
    """An EVIL peer: flips one bit in chunk 1."""

    def _read_chunk(self, path, manifest, index):
        data = bytearray(super()._read_chunk(path, manifest, index))
        if index == 1:
            data[0] ^= 0x01
        return bytes(data)


class DyingServer(PeerServer):
    """A peer that serves 2 chunks, then dies."""

    def _read_chunk(self, path, manifest, index):
        if self.stats["chunks_served"] >= 2:
            self.stop()
            raise ConnectionResetError
        return super()._read_chunk(path, manifest, index)


def make_server(tmp_path, name, data, cls=PeerServer):
    share = tmp_path / f"share_{name}"
    share.mkdir()
    (share / "movie.bin").write_bytes(data)
    srv = cls(share, chunk_size=CS).start()
    return srv, srv.share(share / "movie.bin")


def src(server):
    return Source("127.0.0.1", server.port)


@pytest.fixture
def data():
    return os.urandom(CS * 5 + 1234)  # 6 chunks, the last one is 1234 bytes


@pytest.fixture
def servers():
    started = []
    yield started
    for s in started:
        s.stop()


# ----------------------------------------------------------- source checks
@pytest.mark.parametrize("host,port", [
    ("evil.example.com", 80),   # a name, not an IP
    ("127.0.0.1", 0),           # port 0 is not a real port
    ("127.0.0.1", 70000),       # too big
    ("0.0.0.0", 80),            # "any address" is not a real peer
    ("127.0.0.1", "80"),        # text, not a number
])
def test_bad_sources_rejected(host, port):
    with pytest.raises(DownloadError):
        Source.validated(host, port)


def test_good_source_accepted():
    s = Source.validated("192.168.1.20", 9001, "abc")
    assert (s.host, s.port, s.peer_id) == ("192.168.1.20", 9001, "abc")


# ----------------------------------------------------------- happy path
def test_single_source_download(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    progress = []
    res = download(m, [src(srv)], tmp_path / "dl", progress=lambda d, t, s: progress.append(d))
    assert res.path.read_bytes() == data
    assert res.path.name == "movie.bin"
    assert progress == [1, 2, 3, 4, 5, 6]
    assert not list((tmp_path / "dl").glob("*.part"))   # temp file cleaned up


def test_existing_file_is_not_overwritten(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    dl = tmp_path / "dl"
    dl.mkdir()
    (dl / "movie.bin").write_text("my own file")
    res = download(m, [src(srv)], dl)
    assert res.path.name == "movie (1).bin"
    assert (dl / "movie.bin").read_text() == "my own file"


def test_many_downloads_at_once(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    results, errors = [], []

    def worker(i):
        try:
            results.append(download(m, [src(srv)], tmp_path / f"dl{i}"))
        except Exception as e:
            errors.append(e)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(results) == 8
    assert all(hashlib.sha256(r.path.read_bytes()).hexdigest() == m.file_id for r in results)


# ----------------------------------------------------------- when things go wrong
def test_evil_peer_caught_and_chunk_fetched_from_honest_peer(tmp_path, data, servers):
    evil, m = make_server(tmp_path, "evil", data, CorruptingServer)
    good, _ = make_server(tmp_path, "good", data)
    servers += [evil, good]
    res = download(m, [src(evil), src(good)], tmp_path / "dl")
    assert res.path.read_bytes() == data                          # file is perfect
    assert "hash mismatch" in res.failed_sources[f"127.0.0.1:{evil.port}"]
    assert res.chunks_by_source == {f"127.0.0.1:{evil.port}": 1,   # chunk 0 only
                                    f"127.0.0.1:{good.port}": 5}   # chunks 1-5


def test_evil_peer_alone_fails_cleanly(tmp_path, data, servers):
    evil, m = make_server(tmp_path, "evil", data, CorruptingServer)
    servers.append(evil)
    with pytest.raises(DownloadError):
        download(m, [src(evil)], tmp_path / "dl")
    assert not (tmp_path / "dl" / "movie.bin").exists()   # no broken file under the real name


def test_peer_dies_midway_and_download_continues(tmp_path, data, servers):
    dying, m = make_server(tmp_path, "dying", data, DyingServer)
    good, _ = make_server(tmp_path, "good", data)
    servers += [dying, good]
    res = download(m, [src(dying), src(good)], tmp_path / "dl", io_timeout=3)
    assert res.path.read_bytes() == data
    assert res.chunks_by_source[f"127.0.0.1:{dying.port}"] == 2


def test_offline_peer_skipped(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    with socket.socket() as s:            # find a port nobody listens on
        s.bind(("127.0.0.1", 0))
        dead_port = s.getsockname()[1]
    res = download(m, [Source("127.0.0.1", dead_port), src(srv)], tmp_path / "dl")
    assert res.path.read_bytes() == data
    assert f"127.0.0.1:{dead_port}" in res.failed_sources


def test_manifest_for_a_different_file_rejected(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    with pytest.raises(IntegrityError):
        download(m, [src(srv)], tmp_path / "dl", expected_file_id="0" * 64)


def test_no_sources(tmp_path, data, servers):
    srv, m = make_server(tmp_path, "a", data)
    servers.append(srv)
    with pytest.raises(DownloadError):
        download(m, [], tmp_path / "dl")