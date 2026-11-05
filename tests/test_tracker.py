import os
import sqlite3

import pytest

from chunkswarm.manifest import MIN_CHUNK_SIZE, build_manifest
from chunkswarm.tracker import create_app

CS = MIN_CHUNK_SIZE


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    app = create_app(tmp_path / "t.db", peer_ttl=30, chunk_size=CS, clock=clock)
    return app.test_client(), clock, tmp_path


def register(client, name="p", port=9000):
    r = client.post("/api/peers/register", json={"name": name, "port": port})
    assert r.status_code == 201
    return {"Authorization": f"Bearer {r.json['token']}"}, r.json["peer_id"]


def manifest_for(tmp_path, name="notes.pdf", size=CS * 2 + 7):
    p = tmp_path / name
    p.write_bytes(os.urandom(size))
    return build_manifest(p, CS)


def test_full_flow_register_share_search_sources(env):
    c, _, tmp = env
    a, a_id = register(c, "A", 9001)
    b, _ = register(c, "B", 9002)
    m = manifest_for(tmp)
    assert c.post("/api/files/share", json={"manifest": m.to_dict()}, headers=a).status_code == 201

    res = c.get("/api/files/search?q=notes", headers=b).json["results"]
    assert [r["file_id"] for r in res] == [m.file_id] and res[0]["sources"] == 1

    srcs = c.get(f"/api/files/{m.file_id}/sources", headers=b).json["sources"]
    assert srcs == [{"peer_id": a_id, "host": "127.0.0.1", "port": 9001}]
    got = c.get(f"/api/files/{m.file_id}/manifest", headers=b).json["manifest"]
    assert got == m.to_dict()

    # A peer never sees itself as a source or a search hit.
    assert c.get("/api/files/search?q=notes", headers=a).json["results"] == []


@pytest.mark.parametrize("method,path", [
    ("get", "/api/files/search?q=x"),
    ("post", "/api/peers/heartbeat"),
    ("post", "/api/files/share"),
    ("get", f"/api/files/{'a' * 64}/sources"),
])
def test_endpoints_require_auth(env, method, path):
    c, _, _ = env
    assert getattr(c, method)(path).status_code == 401
    bad = {"Authorization": "Bearer deadbeef.wrongsecret"}
    assert getattr(c, method)(path, headers=bad).status_code == 401


def test_token_is_not_stored_in_plaintext(env):
    c, _, tmp = env
    r = c.post("/api/peers/register", json={"name": "A", "port": 9001})
    secret = r.json["token"].split(".", 1)[1]
    dump = "\n".join(sqlite3.connect(tmp / "t.db").iterdump())
    assert secret not in dump


@pytest.mark.parametrize("payload", [
    {"name": "A", "port": 0},
    {"name": "A", "port": 70000},
    {"name": "A", "port": "9000"},
    {"name": "x" * 65, "port": 9000},
    {"name": "bad\nname", "port": 9000},
])
def test_register_validation(env, payload):
    c, _, _ = env
    assert c.post("/api/peers/register", json=payload).status_code == 400


def test_host_comes_from_connection_not_body(env):
    c, _, tmp = env
    a, _ = register(c, "A", 9001)
    c.post("/api/peers/register", json={"name": "evil", "port": 1, "host": "10.9.9.9"})
    b, _ = register(c, "B")
    m = manifest_for(tmp)
    c.post("/api/files/share", json={"manifest": m.to_dict()}, headers=a)
    hosts = {s["host"] for s in c.get(f"/api/files/{m.file_id}/sources", headers=b).json["sources"]}
    assert hosts == {"127.0.0.1"}


def test_conflicting_manifest_rejected(env):
    c, _, tmp = env
    a, _ = register(c, "A")
    b, _ = register(c, "B")
    m = manifest_for(tmp).to_dict()
    assert c.post("/api/files/share", json={"manifest": m}, headers=a).status_code == 201
    forged = dict(m, chunk_hashes=["0" * 64] * len(m["chunk_hashes"]))
    assert c.post("/api/files/share", json={"manifest": forged}, headers=b).status_code == 409


def test_wrong_chunk_size_and_garbage_manifest_rejected(env, tmp_path):
    c, _, _ = env
    a, _ = register(c)
    p = tmp_path / "x"
    p.write_bytes(b"abc")
    big = build_manifest(p, CS * 2).to_dict()
    assert c.post("/api/files/share", json={"manifest": big}, headers=a).status_code == 400
    assert c.post("/api/files/share", json={"manifest": "nope"}, headers=a).status_code == 400
    assert c.post("/api/files/share", data="not json", headers=a).status_code == 400


def test_search_escapes_like_wildcards(env):
    c, _, tmp = env
    a, _ = register(c, "A")
    b, _ = register(c, "B")
    for name in ("report.pdf", "100%_done.txt"):
        c.post("/api/files/share", json={"manifest": manifest_for(tmp, name).to_dict()}, headers=a)
    names = lambda q: [r["name"] for r in c.get("/api/files/search", query_string={"q": q}, headers=b).json["results"]]
    assert names("%") == ["100%_done.txt"]
    assert names("_") == ["100%_done.txt"]
    assert names("' OR 1=1 --") == []
    assert sorted(names("")) == ["100%_done.txt", "report.pdf"]


def test_invalid_file_id_rejected(env):
    c, _, _ = env
    a, _ = register(c)
    assert c.get("/api/files/..%2F..%2Fetc/sources", headers=a).status_code in (400, 404)
    assert c.get(f"/api/files/{'Z' * 64}/manifest", headers=a).status_code == 400


def test_dead_peers_expire_with_their_files(env):
    c, clock, tmp = env
    a, _ = register(c, "A")
    b, _ = register(c, "B")
    m = manifest_for(tmp)
    c.post("/api/files/share", json={"manifest": m.to_dict()}, headers=a)

    clock.t += 20
    c.post("/api/peers/heartbeat", headers=b)        # B stays alive, A goes silent
    clock.t += 20                                     # A last seen 40s ago > TTL 30s
    assert c.get("/api/files/search?q=", headers=b).json["results"] == []
    assert c.post("/api/peers/heartbeat", headers=a).status_code == 401  # A's record is gone
    assert c.get("/api/health").json["peers"] == 1


def test_unregister_removes_shares(env):
    c, _, tmp = env
    a, _ = register(c, "A")
    b, _ = register(c, "B")
    m = manifest_for(tmp)
    c.post("/api/files/share", json={"manifest": m.to_dict()}, headers=a)
    c.post("/api/peers/unregister", headers=a)
    assert c.get(f"/api/files/{m.file_id}/manifest", headers=b).status_code == 404


def test_body_size_limit(env):
    c, _, _ = env
    a, _ = register(c)
    r = c.post("/api/files/share", data=b"{" + b" " * (2 * 1024 * 1024) + b"}",
               headers={**a, "Content-Type": "application/json"})
    assert r.status_code == 413


def test_rate_limit(tmp_path):
    app = create_app(tmp_path / "r.db", rate_limit=5, rate_window=60)
    c = app.test_client()
    codes = [c.get("/api/health").status_code for _ in range(7)]
    assert codes[:5] == [200] * 5 and codes[5:] == [429, 429]


def test_errors_do_not_leak_internals(env):
    c, _, _ = env
    r = c.get("/api/does-not-exist")
    assert r.status_code == 404 and "Traceback" not in r.get_data(as_text=True)