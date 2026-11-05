"""Tests for parallel multi-source downloads."""

import os
import time

import pytest

from chunkswarm.downloader import DownloadError, Source, download
from chunkswarm.manifest import MIN_CHUNK_SIZE
from chunkswarm.peer_server import PeerServer

CS = MIN_CHUNK_SIZE          # 64 KiB chunks
LIMIT = 2 * 1024 * 1024      # each test peer uploads at most 2 MiB/s


class CorruptingServer(PeerServer):
    """An evil peer: every chunk it sends has one flipped bit."""

    def _read_chunk(self, path, manifest, index):
        data = bytearray(super()._read_chunk(path, manifest, index))
        data[0] ^= 0x01
        return bytes(data)


class DyingServer(PeerServer):
    """Serves 2 chunks, then dies."""

    def _read_chunk(self, path, manifest, index):
        if self.stats["chunks_served"] >= 2:
            self.stop()
            raise ConnectionResetError
        return super()._read_chunk(path, manifest, index)


@pytest.fixture
def data():
    return os.urandom(CS * 24)   # 24 chunks = 1.5 MiB


@pytest.fixture
def make(tmp_path):
    started = []

    def _make(name, data, cls=PeerServer, limit=LIMIT):
        share = tmp_path / f"share_{name}"
        share.mkdir()
        (share / "movie.bin").write_bytes(data)
        srv = cls(share, chunk_size=CS, upload_limit=limit).start()
        started.append(srv)
        return srv, srv.share(share / "movie.bin")

    yield _make
    for s in started:
        s.stop()


def src(server):
    return Source("127.0.0.1", server.port)


def label(server):
    return f"127.0.0.1:{server.port}"


def test_three_sources_all_help(tmp_path, data, make):
    peers = [make(n, data) for n in "abc"]
    m = peers[0][1]
    res = download(m, [src(p) for p, _ in peers], tmp_path / "dl")
    assert res.path.read_bytes() == data
    assert all(res.chunks_by_source.get(label(p), 0) >= 1 for p, _ in peers)
    assert sum(res.chunks_by_source.values()) == 24


def test_three_sources_are_faster_than_one(tmp_path, data, make):
    peers = [make(n, data) for n in "abc"]
    m = peers[0][1]
    one = download(m, [src(peers[0][0])], tmp_path / "dl1")
    three = download(m, [src(p) for p, _ in peers], tmp_path / "dl3")
    assert three.seconds < one.seconds * 0.6, (one.seconds, three.seconds)


def test_max_workers_1_uses_one_peer_at_a_time(tmp_path, data, make):
    peers = [make(n, data) for n in "ab"]
    m = peers[0][1]
    res = download(m, [src(p) for p, _ in peers], tmp_path / "dl", max_workers=1)
    assert res.path.read_bytes() == data
    assert res.chunks_by_source == {label(peers[0][0]): 24}


def test_peer_dies_others_finish_its_work(tmp_path, data, make):
    dying, m = make("dying", data, DyingServer)
    good1, _ = make("good1", data)
    good2, _ = make("good2", data)
    res = download(m, [src(dying), src(good1), src(good2)], tmp_path / "dl", io_timeout=3)
    assert res.path.read_bytes() == data
    assert res.chunks_by_source[label(dying)] == 2
    assert label(dying) in res.failed_sources


def test_evil_peer_is_dropped_and_its_chunk_refetched(tmp_path, data, make):
    evil, m = make("evil", data, CorruptingServer)
    good1, _ = make("good1", data)
    good2, _ = make("good2", data)
    res = download(m, [src(evil), src(good1), src(good2)], tmp_path / "dl")
    assert res.path.read_bytes() == data
    assert "hash mismatch" in res.failed_sources[label(evil)]
    assert label(evil) not in res.chunks_by_source        # not one bad chunk was kept


def test_all_sources_bad_fails_but_keeps_progress(tmp_path, data, make):
    d1, m = make("d1", data, DyingServer)
    d2, _ = make("d2", data, DyingServer)
    dl = tmp_path / "dl"
    with pytest.raises(DownloadError):
        download(m, [src(d1), src(d2)], dl, io_timeout=3)
    assert (dl / f"{m.file_id}.part").exists()
    assert (dl / f"{m.file_id}.state.json").exists()     # 4 good chunks can be resumed
    good, _ = make("good", data)
    res = download(m, [src(good)], dl)
    assert res.path.read_bytes() == data
    assert res.resumed_chunks == 4


def test_upload_limit_slows_a_peer_down(tmp_path, make):
    data = os.urandom(CS * 8)                            # 512 KiB
    slow, m = make("slow", data, limit=1024 * 1024)      # 1 MiB/s -> about 0.5 s
    t = time.perf_counter()
    download(m, [src(slow)], tmp_path / "dl")
    assert time.perf_counter() - t >= 0.4