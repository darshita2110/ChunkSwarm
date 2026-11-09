"""Tests for the web dashboard: a real tracker and two real peers, driven through HTTP."""

import os
import threading
import time

import pytest
from werkzeug.serving import make_server

from chunkswarm.dashboard import Dashboard, create_dashboard_app
from chunkswarm.manifest import MIN_CHUNK_SIZE, Manifest
from chunkswarm.peer import PeerNode
from chunkswarm.tracker import create_app

EVIL_NAME = "<img src=x onerror=alert(1)>.txt"


@pytest.fixture
def env(tmp_path):
    tracker = make_server("127.0.0.1", 0, create_app(tmp_path / "t.db", chunk_size=MIN_CHUNK_SIZE),
                          threaded=True)
    threading.Thread(target=tracker.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{tracker.server_port}"

    data = os.urandom(MIN_CHUNK_SIZE * 10 + 7)
    (tmp_path / "A").mkdir()
    (tmp_path / "A" / "notes.pdf").write_bytes(data)
    a = PeerNode(url, tmp_path / "A", tmp_path / "A_dl", name="A").start()
    b = PeerNode(url, tmp_path / "B", tmp_path / "B_dl", name="B").start()
    dash = Dashboard(b, tmp_path / "b.db")
    client = create_dashboard_app(dash).test_client()
    yield client, dash, data, tmp_path, a
    for n in (a, b):
        n.stop()
    tracker.shutdown()


def post(client, path, json, token, **kw):
    return client.post(path, json=json, headers={"X-ChunkSwarm-Token": token}, **kw)


def wait_for_job(client, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        jobs = client.get("/api/downloads").json["jobs"]
        if jobs and jobs[0]["status"] != "running":
            return jobs[0]
        time.sleep(0.05)
    raise AssertionError("download did not finish")


def test_page_contains_the_session_token(env):
    client, dash, *_ = env
    page = client.get("/").get_data(as_text=True)
    assert dash.token in page and "{{TOKEN}}" not in page


def test_status_and_files(env):
    client, *_ = env
    s = client.get("/api/status").json
    assert s["name"] == "B" and s["tracker_ok"] is True and s["files_shared"] == 0


def test_search_then_download_through_the_dashboard(env):
    client, dash, data, tmp_path, _ = env
    results = client.get("/api/search?q=notes").json["results"]
    assert [r["name"] for r in results] == ["notes.pdf"]
    r = post(client, "/api/downloads", {"file_id": results[0]["file_id"]}, dash.token)
    assert r.status_code == 202
    job = wait_for_job(client)
    assert job["status"] == "done" and job["done"] == job["total"] == 11
    assert all(job["owners"])                                   # every chunk has an owner
    assert (tmp_path / "B_dl" / "notes.pdf").read_bytes() == data
    history = client.get("/api/downloads").json["history"]
    assert history[0]["name"] == "notes.pdf" and history[0]["status"] == "done"


def test_history_survives_a_restart(env):
    client, dash, _, tmp_path, _ = env
    fid = client.get("/api/search?q=").json["results"][0]["file_id"]
    post(client, "/api/downloads", {"file_id": fid}, dash.token)
    wait_for_job(client)
    again = Dashboard(dash.node, tmp_path / "b.db")              # same database file
    assert again.history()[0]["file_id"] == fid


def test_post_without_token_is_refused(env):                   # CSRF
    client, *_ = env
    r = client.post("/api/downloads", json={"file_id": "a" * 64})
    assert r.status_code == 403
    r = post(client, "/api/rescan", {}, "wrong-token")
    assert r.status_code == 403


@pytest.mark.parametrize("host", ["evil.example.com", "evil.example.com:8080", "192.168.1.5"])
def test_foreign_host_header_is_refused(env, host):           # DNS rebinding
    client, *_ = env
    assert client.get("/api/status", headers={"Host": host}).status_code == 403


@pytest.mark.parametrize("host", ["127.0.0.1:8080", "localhost:8080", "localhost"])
def test_local_host_headers_are_allowed(env, host):
    client, *_ = env
    assert client.get("/api/status", headers={"Host": host}).status_code == 200


@pytest.mark.parametrize("file_id", ["../../etc/passwd", "a" * 63, None, 123])
def test_bad_file_id_rejected(env, file_id):
    client, dash, *_ = env
    assert post(client, "/api/downloads", {"file_id": file_id}, dash.token).status_code == 400


def test_unknown_file_gives_a_clear_error(env):
    client, dash, *_ = env
    r = post(client, "/api/downloads", {"file_id": "a" * 64}, dash.token)
    assert r.status_code == 502 and "404" in r.json["error"]


def test_evil_file_name_is_only_ever_data(env):                # XSS
    client, dash, _, tmp_path, a = env
    (tmp_path / "A" / "evil.txt").write_text("hi")
    real = a.server.share(tmp_path / "A" / "evil.txt")
    evil = Manifest(real.file_id, EVIL_NAME, real.size, real.chunk_size, real.chunk_hashes)
    a.tracker.share(evil)                    # a peer advertises the file under a nasty name
    r = client.get("/api/search?q=onerror")
    assert r.mimetype == "application/json"
    names = [x["name"] for x in r.json["results"]]
    assert names and all("<" not in n and ">" not in n for n in names)   # tracker cleaned it
    js = client.get("/static/app.js").get_data(as_text=True)
    assert "innerHTML" not in js and "insertAdjacentHTML" not in js     # page never parses HTML


def test_security_headers(env):
    client, *_ = env
    h = client.get("/").headers
    assert h["X-Frame-Options"] == "DENY"
    assert "script-src 'self'" in h["Content-Security-Policy"]


def test_rescan_shares_new_files(env):
    client, dash, _, tmp_path, _ = env
    (tmp_path / "B").mkdir(exist_ok=True)
    (tmp_path / "B" / "new.txt").write_text("hello")
    assert post(client, "/api/rescan", {}, dash.token).json["shared"] == 1
    assert client.get("/api/status").json["files_shared"] == 1
