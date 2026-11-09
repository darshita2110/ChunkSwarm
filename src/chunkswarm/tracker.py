"""ChunkSwarm tracker: the "phone book" of the network.

It knows WHO is online and WHO has WHICH file. It never sees file contents:
files go directly from peer to peer.

    peer A --POST /api/peers/register------> tracker   "I'm online, port 9001"
    peer A --POST /api/files/share---------> tracker   "I have notes.pdf (manifest)"
    peer B --GET  /api/files/search?q=notes> tracker   "who has notes?"
    peer B --GET  /api/files/<id>/sources--> tracker   "A, at 192.168.1.5:9001"
    peer B ====== TCP, our own protocol =====> peer A   (the actual file)

Security and privacy decisions
------------------------------
- Registering gives a peer a secret token "<peer_id>.<secret>". We store only
  the SHA-256 of the secret, so a stolen database contains no usable tokens.
- Every other endpoint needs that token.
- A peer's IP comes from the TCP connection, NEVER from the JSON body
  (otherwise anyone could point all downloads at someone else's computer).
- A peer that stops sending heartbeats is deleted, with its IP and its shares.
- All SQL uses "?" placeholders (no SQL injection).

YOUR TASK: TODOs A to F. Run `pytest tests/test_tracker.py -v`.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import secrets
import sqlite3
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Callable

from flask import Flask, g, jsonify, request
from werkzeug.exceptions import HTTPException

from .manifest import DEFAULT_CHUNK_SIZE, Manifest, ManifestError, is_file_id

log = logging.getLogger("chunkswarm.tracker")

# Three tables. "shares" links peers to files (many-to-many):
#   peer A has notes.pdf and talk.pptx, and notes.pdf is also on peer B.
SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS peers (
    peer_id     TEXT PRIMARY KEY,
    token_hash  TEXT NOT NULL,
    name        TEXT NOT NULL,
    host        TEXT NOT NULL,
    port        INTEGER NOT NULL,
    last_seen   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    file_id     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    chunk_size  INTEGER NOT NULL,
    manifest    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shares (
    peer_id  TEXT NOT NULL REFERENCES peers(peer_id) ON DELETE CASCADE,
    file_id  TEXT NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    PRIMARY KEY (peer_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
"""


class ApiError(Exception):
    """Raise this inside an endpoint to reply {"error": message} with a status code."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


class RateLimiter:
    """At most `limit` requests per `window` seconds from one IP."""

    def __init__(self, limit: int, window: float, clock: Callable[[], float]):
        self.limit, self.window, self.clock = limit, window, clock
        self._hits: dict[str, deque] = defaultdict(deque)   # ip -> times of recent requests
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = self.clock()
        with self._lock:
            hits = self._hits[key]
            while hits and hits[0] <= now - self.window:   # forget requests older than the window
                hits.popleft()
            if len(hits) >= self.limit:
                return False
            hits.append(now)
            return True


def create_app(db_path: str | Path, *, peer_ttl: float = 30.0, chunk_size: int = DEFAULT_CHUNK_SIZE,
               rate_limit: int = 300, rate_window: float = 60.0,
               clock: Callable[[], float] = time.time) -> Flask:
    """Build the tracker app. (`clock` is replaceable so tests can fast-forward time.)"""
    app = Flask(__name__)
    app.config.update(
        MAX_CONTENT_LENGTH=1024 * 1024,   # reject request bodies over 1 MiB
        DB_PATH=str(db_path),
        PEER_TTL=peer_ttl,                # seconds without a heartbeat before a peer is "dead"
        CHUNK_SIZE=chunk_size,            # every peer must chunk files the same way
    )
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
    try:
        os.chmod(db_path, 0o600)          # only our OS user may read the database
    except OSError:
        pass
    limiter = RateLimiter(rate_limit, rate_window, clock)

    # ------------------------------------------------------------ plumbing
    def db() -> sqlite3.Connection:
        """One database connection per request (stored on Flask's `g`)."""
        if "db" not in g:
            g.db = sqlite3.connect(app.config["DB_PATH"], timeout=5)
            g.db.row_factory = sqlite3.Row             # rows behave like dicts: row["name"]
            g.db.execute("PRAGMA foreign_keys = ON")   # SQLite has this OFF by default!
        return g.db

    @app.teardown_appcontext
    def _close_db(_exc):
        conn = g.pop("db", None)
        if conn is not None:
            conn.close()

    @app.before_request
    def _guard():
        """Runs before EVERY request."""
        if not limiter.allow(request.remote_addr or "?"):
            raise ApiError(429, "rate limit exceeded")
        # TODO C: delete dead peers, then files nobody shares any more
        conn = db()
        conn.execute("DELETE FROM peers WHERE last_seen < ?", (clock() - app.config["PEER_TTL"],))
        conn.execute("DELETE FROM files WHERE file_id NOT IN (SELECT file_id FROM shares)")
        conn.commit()

    @app.after_request
    def _headers(resp):
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.errorhandler(ApiError)
    def _api_error(e: ApiError):
        return jsonify(error=e.message), e.status

    @app.errorhandler(HTTPException)
    def _http_error(e: HTTPException):
        return jsonify(error=e.name), e.code

    @app.errorhandler(Exception)
    def _internal(e: Exception):
        log.exception("unhandled error")
        return jsonify(error="internal error"), 500   # never show a traceback to clients

    def body() -> dict:
        """The request's JSON body, which must be an object."""
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise ApiError(400, "JSON object body required")
        return data

    def authenticate() -> sqlite3.Row:
        """Check the 'Authorization: Bearer <peer_id>.<secret>' header.

        Return the peer's database row, or raise ApiError(401, ...).
        """
        # TODO B
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise ApiError(401, "missing bearer token")
        peer_id, _, secret = auth[7:].partition(".")
        if not peer_id or not secret:
            raise ApiError(401, "malformed token")
        row = db().execute("SELECT * FROM peers WHERE peer_id = ?", (peer_id,)).fetchone()
        if row is None or not hmac.compare_digest(row["token_hash"], _hash_secret(secret)):
            raise ApiError(401, "invalid or expired token")
        return row

    # ----------------------------------------------------------- endpoints
    @app.post("/api/peers/register")
    def register():
        """Body: {"name": "A", "port": 9001}. Returns peer_id + token (201)."""
        # TODO A
        data = body()
        name, port = data.get("name", "peer"), data.get("port")
        if not isinstance(name, str) or not 1 <= len(name) <= 64 or not name.isprintable():
            raise ApiError(400, "name must be 1-64 printable characters")
        if type(port) is not int or not 1 <= port <= 65535:
            raise ApiError(400, "port must be an integer 1-65535")
        peer_id = secrets.token_hex(16)
        secret = secrets.token_urlsafe(32)
        conn = db()
        conn.execute(
            "INSERT INTO peers (peer_id, token_hash, name, host, port, last_seen) VALUES (?, ?, ?, ?, ?, ?)",
            (peer_id, _hash_secret(secret), name, request.remote_addr, port, clock()))
        conn.commit()
        return jsonify(peer_id=peer_id, token=f"{peer_id}.{secret}",
                       heartbeat_interval=max(1.0, app.config["PEER_TTL"] / 3),
                       chunk_size=app.config["CHUNK_SIZE"]), 201
    

    @app.post("/api/peers/heartbeat")
    def heartbeat():
        """'I'm still alive.' Updates last_seen."""
        # TODO B
        peer = authenticate()
        conn = db()
        conn.execute("UPDATE peers SET last_seen = ? WHERE peer_id = ?", (clock(), peer["peer_id"]))
        conn.commit()
        return jsonify(ok=True)

    @app.post("/api/peers/unregister")
    def unregister():
        """'I'm leaving.' Deletes the peer (its shares go with it)."""
        # TODO B
        peer = authenticate()
        conn = db()
        conn.execute("DELETE FROM peers WHERE peer_id = ?", (peer["peer_id"],))
        conn.execute("DELETE FROM files WHERE file_id NOT IN (SELECT file_id FROM shares)")
        conn.commit()
        return jsonify(ok=True)

    @app.post("/api/files/share")
    def share():
        """Body: {"manifest": {...}}. Records that this peer has this file (201)."""
        # TODO D
        peer = authenticate()
        try:
            manifest = Manifest.from_dict(body().get("manifest"))
        except ManifestError as e:
            raise ApiError(400, f"invalid manifest: {e}")
        if manifest.chunk_size != app.config["CHUNK_SIZE"]:
            raise ApiError(400, f"chunk_size must be {app.config['CHUNK_SIZE']}")
        conn = db()
        existing = conn.execute("SELECT manifest FROM files WHERE file_id = ?",
                                (manifest.file_id,)).fetchone()
        if existing is None:
            conn.execute("INSERT INTO files (file_id, name, size, chunk_size, manifest) VALUES (?, ?, ?, ?, ?)",
                         (manifest.file_id, manifest.name, manifest.size, manifest.chunk_size,
                          json.dumps(manifest.to_dict())))
        else:
            known = Manifest.from_dict(json.loads(existing["manifest"]))
            if (known.size, known.chunk_hashes) != (manifest.size, manifest.chunk_hashes):
                raise ApiError(409, "manifest conflicts with the one already registered")
        conn.execute("INSERT OR IGNORE INTO shares (peer_id, file_id) VALUES (?, ?)",
                     (peer["peer_id"], manifest.file_id))
        conn.commit()
        return jsonify(file_id=manifest.file_id), 201

    @app.get("/api/files/search")
    def search():
        """?q=notes -> files whose name contains 'notes', shared by OTHER peers."""
        # TODO E
        peer = authenticate()
        q = request.args.get("q", "")
        if len(q) > 100:
            raise ApiError(400, "query too long")
        pattern = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows = db().execute(
            """SELECT f.file_id, f.name, f.size, COUNT(s.peer_id) AS sources
                 FROM files f JOIN shares s ON s.file_id = f.file_id
                WHERE f.name LIKE ? ESCAPE '\\' AND s.peer_id != ?
                GROUP BY f.file_id ORDER BY f.name LIMIT 50""",
            (pattern, peer["peer_id"])).fetchall()
        return jsonify(results=[dict(r) for r in rows])

    @app.get("/api/files/<file_id>/manifest")
    def manifest(file_id: str):
        """The trusted manifest for one file."""
        # TODO F
        authenticate()
        if not is_file_id(file_id):
            raise ApiError(400, "invalid file id")
        row = db().execute("SELECT manifest FROM files WHERE file_id = ?", (file_id,)).fetchone()
        if row is None:
            raise ApiError(404, "unknown file")
        return jsonify(manifest=json.loads(row["manifest"]))

    @app.get("/api/files/<file_id>/sources")
    def sources(file_id: str):
        """Which OTHER live peers have this file: [{peer_id, host, port}, ...]."""
        # TODO F
        peer = authenticate()
        if not is_file_id(file_id):
            raise ApiError(400, "invalid file id")
        rows = db().execute(
            """SELECT p.peer_id, p.host, p.port
                 FROM shares s JOIN peers p ON p.peer_id = s.peer_id
                WHERE s.file_id = ? AND p.peer_id != ?
                ORDER BY p.last_seen DESC""",
            (file_id, peer["peer_id"])).fetchall()
        return jsonify(sources=[dict(r) for r in rows])

    @app.get("/api/health")
    def health():
        live = db().execute("SELECT COUNT(*) FROM peers").fetchone()[0]
        return jsonify(ok=True, peers=live)   # a count only, no IP addresses

    return app


def main() -> None:
    ap = argparse.ArgumentParser(description="ChunkSwarm tracker")
    ap.add_argument("--host", default="127.0.0.1",
                    help="interface to bind (use your LAN IP for multi-laptop demos)")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", default="data/tracker.db")
    ap.add_argument("--peer-ttl", type=float, default=30.0)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    app = create_app(args.db, peer_ttl=args.peer_ttl)
    # Flask's built-in server is fine for a LAN demo (not for production).
    app.run(host=args.host, port=args.port, threaded=True, debug=False)


if __name__ == "__main__":
    main()