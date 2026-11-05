"""Full system: a real HTTP tracker + three real peers talking over TCP."""

import os
import threading

import pytest
from werkzeug.serving import make_server

from chunkswarm.manifest import MIN_CHUNK_SIZE
from chunkswarm.peer import PeerNode, TrackerError
from chunkswarm.tracker import create_app


@pytest.fixture
def tracker_url(tmp_path):
    app = create_app(tmp_path / "tracker.db", chunk_size=MIN_CHUNK_SIZE)
    server = make_server("127.0.0.1", 0, app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def node(tmp_path, url, name, files=None):
    share = tmp_path / name / "shared"
    share.mkdir(parents=True)
    for fname, content in (files or {}).items():
        (share / fname).write_bytes(content)
    return PeerNode(url, share, tmp_path / name / "downloads", name=name).start()


def test_search_and_download_across_peers(tmp_path, tracker_url):
    notes = os.urandom(MIN_CHUNK_SIZE * 7 + 99)
    a = node(tmp_path, tracker_url, "A", {"notes.pdf": notes})
    b = node(tmp_path, tracker_url, "B", {"notes.pdf": notes, "talk.pptx": b"slides" * 1000})
    c = node(tmp_path, tracker_url, "C")
    try:
        hits = {r["name"]: r for r in c.tracker.search("")}
        assert set(hits) == {"notes.pdf", "talk.pptx"}
        assert hits["notes.pdf"]["sources"] == 2  # same content => same file_id on A and B

        res = c.get(hits["notes.pdf"]["file_id"])
        assert res.path.read_bytes() == notes
        assert res.path.parent == tmp_path / "C" / "downloads"

        # Source A leaves; download still works from B.
        a.stop()
        res2 = c.get(hits["notes.pdf"]["file_id"])
        assert res2.path.read_bytes() == notes
        assert all(int(k.split(":")[1]) == b.server.port for k in res2.chunks_by_source)
    finally:
        for n in (b, c):
            n.stop()


def test_invalid_file_id_never_reaches_tracker(tmp_path, tracker_url):
    c = node(tmp_path, tracker_url, "C")
    try:
        with pytest.raises(ValueError):
            c.get("../../etc/passwd")
        with pytest.raises(TrackerError, match="404"):
            c.get("a" * 64)
    finally:
        c.stop()