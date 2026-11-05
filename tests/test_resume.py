"""Tests for resuming interrupted downloads."""

import json
import os

import pytest

from chunkswarm.downloader import DownloadError, Source, download
from chunkswarm.manifest import MIN_CHUNK_SIZE
from chunkswarm.peer_server import PeerServer

CS = MIN_CHUNK_SIZE


class DyingServer(PeerServer):
    """Serves 2 chunks, then dies (like a laptop that loses Wi-Fi)."""

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
    return os.urandom(CS * 5 + 1234)  # 6 chunks


@pytest.fixture
def servers():
    started = []
    yield started
    for s in started:
        s.stop()


def interrupted_download(tmp_path, data, servers):
    """Start a download that dies after 2 chunks. Returns (manifest, download dir)."""
    dying, m = make_server(tmp_path, "dying", data, DyingServer)
    servers.append(dying)
    dl = tmp_path / "dl"
    with pytest.raises(DownloadError):
        download(m, [src(dying)], dl, io_timeout=3)
    return m, dl


def test_interrupted_download_keeps_its_progress(tmp_path, data, servers):
    m, dl = interrupted_download(tmp_path, data, servers)
    assert (dl / f"{m.file_id}.part").exists()
    state = json.loads((dl / f"{m.file_id}.state.json").read_text())
    assert state["file_id"] == m.file_id and state["done"] == [0, 1]
    assert not (dl / "movie.bin").exists()          # never a finished-looking broken file


def test_resume_fetches_only_missing_chunks(tmp_path, data, servers):
    m, dl = interrupted_download(tmp_path, data, servers)
    good, _ = make_server(tmp_path, "good", data)
    servers.append(good)
    res = download(m, [src(good)], dl)
    assert res.path.read_bytes() == data
    assert res.resumed_chunks == 2
    assert sum(res.chunks_by_source.values()) == 4  # only chunks 2-5 were downloaded
    assert list(dl.iterdir()) == [res.path]         # .part and .state.json cleaned up


def test_damaged_chunk_on_disk_is_downloaded_again(tmp_path, data, servers):
    m, dl = interrupted_download(tmp_path, data, servers)
    part = dl / f"{m.file_id}.part"
    with part.open("r+b") as f:                     # damage chunk 1 on disk
        f.seek(CS + 10)
        f.write(b"XXXX")
    good, _ = make_server(tmp_path, "good", data)
    servers.append(good)
    res = download(m, [src(good)], dl)
    assert res.path.read_bytes() == data
    assert res.resumed_chunks == 1                  # only chunk 0 was still good
    assert sum(res.chunks_by_source.values()) == 5


@pytest.mark.parametrize("bad_state", [
    "this is not json",
    json.dumps([1, 2, 3]),
    json.dumps({"file_id": "0" * 64, "chunk_size": CS, "done": [0, 1]}),   # another file
    json.dumps({"file_id": None, "chunk_size": CS, "done": "everything"}),
])
def test_bad_state_file_means_start_fresh(tmp_path, data, servers, bad_state):
    m, dl = interrupted_download(tmp_path, data, servers)
    (dl / f"{m.file_id}.state.json").write_text(bad_state)
    good, _ = make_server(tmp_path, "good", data)
    servers.append(good)
    res = download(m, [src(good)], dl)
    assert res.path.read_bytes() == data
    assert res.resumed_chunks == 0
    assert sum(res.chunks_by_source.values()) == 6


def test_lying_state_file_is_not_trusted(tmp_path, data, servers):
    """The state says every chunk is done, but the .part file is all zeros."""
    m, dl = interrupted_download(tmp_path, data, servers)
    (dl / f"{m.file_id}.part").write_bytes(bytes(m.size))
    (dl / f"{m.file_id}.state.json").write_text(json.dumps(
        {"file_id": m.file_id, "chunk_size": CS, "done": list(range(6))}))
    good, _ = make_server(tmp_path, "good", data)
    servers.append(good)
    res = download(m, [src(good)], dl)
    assert res.path.read_bytes() == data
    assert res.resumed_chunks == 0


def test_fresh_download_reports_nothing_resumed(tmp_path, data, servers):
    good, m = make_server(tmp_path, "good", data)
    servers.append(good)
    res = download(m, [src(good)], tmp_path / "dl")
    assert res.resumed_chunks == 0
    assert list((tmp_path / "dl").iterdir()) == [res.path]