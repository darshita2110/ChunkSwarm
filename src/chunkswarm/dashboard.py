"""ChunkSwarm dashboard: a web page on YOUR computer that controls YOUR peer.

    browser (http://127.0.0.1:8080) <--HTTP/JSON--> this Flask app --> PeerNode
                                                                          |
                                         tracker (HTTP) <-----------------+
                                         other peers (TCP) <--------------+

Endpoints
---------
GET  /                  the page (index.html, with this session's secret token)
GET  /api/status        peer name, ports, tracker reachable, files shared, bytes served
GET  /api/files         files this peer shares
GET  /api/search?q=     search the tracker
POST /api/downloads     start a download  {"file_id": "..."}
GET  /api/downloads     live jobs (with a per-chunk map) + saved history
POST /api/rescan        share any new files in the share folder

Security (the dashboard can start downloads, so it must only obey YOU)
--------
1. Listens on 127.0.0.1 only: other computers cannot reach it.
2. CSRF: any website you visit can make your browser send a POST to
   127.0.0.1:8080. So every POST must carry a secret token in the
   X-ChunkSwarm-Token header. Only our own page knows it (it is put into
   the page when it is served, and other sites cannot read our pages).
3. DNS rebinding: an attacker's domain can be made to point at 127.0.0.1,
   letting their page read ours. We refuse any request whose Host header
   is not 127.0.0.1 or localhost.
4. XSS: file names come from other peers. The page only ever inserts them
   with textContent, never innerHTML, so a name like <img onerror=...>
   is shown as text and never runs.
"""

from __future__ import annotations

import hmac
import secrets
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path

from flask import Flask, Response, jsonify, request
from werkzeug.exceptions import HTTPException
from werkzeug.serving import make_server

from .downloader import DownloadError
from .manifest import is_file_id
from .peer import PeerNode, TrackerError

STATIC = Path(__file__).parent / "static"
ALLOWED_HOSTS = {"127.0.0.1", "localhost"}
SPEED_WINDOW = 2.0          # seconds of history used for the live speed

HISTORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS downloads (
    job_id      TEXT PRIMARY KEY,
    file_id     TEXT NOT NULL,
    name        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    status      TEXT NOT NULL,
    finished    REAL NOT NULL,
    seconds     REAL,
    mb_per_s    REAL,
    sources     INTEGER,
    resumed     INTEGER,
    error       TEXT
);
"""


class DownloadJob:
    """One download started from the dashboard, updated live by the downloader."""

    def __init__(self, job_id: str, file_id: str, name: str, size: int,
                 chunk_size: int, total: int):
        self.job_id, self.file_id, self.name, self.size = job_id, file_id, name, size
        self.chunk_size, self.total = chunk_size, total
        self.status = "running"             # running -> done | failed
        self.done = 0
        self.owners: list[str | None] = [None] * total   # who delivered each chunk
        self.started = time.time()
        self.seconds: float | None = None
        self.mb_per_s: float | None = None   # final average
        self.resumed = 0
        self.failed_sources: dict[str, str] = {}
        self.error: str | None = None
        self.path: str | None = None
        self._samples: deque[tuple[float, int]] = deque()   # (time, chunks done)
        self._lock = threading.Lock()

    # called from downloader threads
    def on_chunk(self, index: int, label: str) -> None:
        with self._lock:
            self.owners[index] = label

    def on_progress(self, done: int, total: int, label: str) -> None:
        now = time.monotonic()
        with self._lock:
            self.done = done
            self._samples.append((now, done))
            while self._samples and now - self._samples[0][0] > SPEED_WINDOW:
                self._samples.popleft()

    def live_speed(self) -> float:
        """MB/s over the last ~2 seconds (not the average since the start)."""
        if self.status != "running" or len(self._samples) < 2:
            return 0.0
        (t0, d0), (t1, d1) = self._samples[0], self._samples[-1]
        if t1 <= t0:
            return 0.0
        return (d1 - d0) * self.chunk_size / 1e6 / (t1 - t0)

    def to_dict(self) -> dict:
        with self._lock:
            sources = sorted({o for o in self.owners if o and o != "resumed"})
            return {
                "job_id": self.job_id, "file_id": self.file_id, "name": self.name,
                "size": self.size, "status": self.status, "done": self.done,
                "total": self.total, "speed": round(self.live_speed(), 2),
                "mb_per_s": self.mb_per_s, "seconds": self.seconds,
                "resumed": self.resumed, "owners": list(self.owners), "sources": sources,
                "failed_sources": dict(self.failed_sources), "error": self.error,
                "path": self.path,
            }


class Dashboard:
    """Keeps the jobs and the history; the Flask app below is a thin layer over it."""

    def __init__(self, node: PeerNode, db_path: Path):
        self.node = node
        self.token = secrets.token_urlsafe(32)      # new every time the peer starts
        self.jobs: dict[str, DownloadJob] = {}
        self._lock = threading.Lock()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(HISTORY_SCHEMA)

    # ------------------------------------------------------------ downloads
    def start_download(self, file_id: str) -> DownloadJob:
        with self._lock:                            # same file already running? reuse it
            for job in self.jobs.values():
                if job.file_id == file_id and job.status == "running":
                    return job
        manifest = self.node.tracker.manifest(file_id)
        job = DownloadJob(secrets.token_hex(8), file_id, manifest.name, manifest.size,
                          manifest.chunk_size, manifest.num_chunks)
        with self._lock:
            self.jobs[job.job_id] = job
        threading.Thread(target=self._run, args=(job,), daemon=True,
                         name=f"job-{job.job_id}").start()
        return job

    def _run(self, job: DownloadJob) -> None:
        try:
            res = self.node.get(job.file_id, progress=job.on_progress, on_chunk=job.on_chunk)
            job.status, job.path = "done", str(res.path)
            job.done, job.seconds = job.total, round(res.seconds, 2)
            job.mb_per_s = round(res.mb_per_s, 2)
            job.resumed, job.failed_sources = res.resumed_chunks, res.failed_sources
        except (DownloadError, TrackerError, OSError, ValueError) as e:
            job.status, job.error = "failed", str(e)
        self._save_history(job)

    def _save_history(self, job: DownloadJob) -> None:
        d = job.to_dict()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO downloads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job.job_id, job.file_id, job.name, job.size, job.status, time.time(),
                 job.seconds, job.mb_per_s, len(d["sources"]), job.resumed, job.error))

    def history(self, limit: int = 20) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM downloads ORDER BY finished DESC LIMIT ?",
                                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def job_list(self) -> list[dict]:
        with self._lock:
            jobs = sorted(self.jobs.values(), key=lambda j: j.started, reverse=True)
        return [j.to_dict() for j in jobs]

    # ------------------------------------------------------------ status
    def status(self) -> dict:
        try:
            self.node.tracker.heartbeat()
            tracker_ok = True
        except TrackerError:
            tracker_ok = False
        stats = self.node.server.stats
        return {
            "name": self.node.name,
            "port": self.node.server.port,
            "tracker": self.node.tracker.base_url,
            "tracker_ok": tracker_ok,
            "files_shared": len(self.node.server.manifests()),
            "bytes_served": stats["bytes_served"],
            "chunks_served": stats["chunks_served"],
            "connections": stats["connections"],
            "rejected": stats["rejected"],
        }

    def rescan(self) -> int:
        manifests = self.node.server.share_all()
        for m in manifests:
            self.node.tracker.share(m)
        return len(manifests)


def create_dashboard_app(dash: Dashboard) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024

    @app.before_request
    def _guard():
        host = (request.host or "").rsplit(":", 1)[0].strip("[]")
        if host not in ALLOWED_HOSTS:                     # DNS rebinding
            return jsonify(error="forbidden host"), 403
        if request.method == "POST":                      # CSRF
            sent = request.headers.get("X-ChunkSwarm-Token", "")
            if not hmac.compare_digest(sent, dash.token):
                return jsonify(error="missing or wrong token"), 403
        return None

    @app.after_request
    def _headers(resp):
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"          # no embedding in other sites
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; style-src 'self'; script-src 'self'")
        return resp

    @app.errorhandler(HTTPException)
    def _http_error(e):
        return jsonify(error=e.name), e.code

    @app.get("/")
    def index():
        html = (STATIC / "index.html").read_text(encoding="utf-8")
        return Response(html.replace("{{TOKEN}}", dash.token), mimetype="text/html")

    @app.get("/api/status")
    def status():
        return jsonify(dash.status())

    @app.get("/api/files")
    def files():
        return jsonify(files=[{"file_id": m.file_id, "name": m.name, "size": m.size,
                               "chunks": m.num_chunks} for m in dash.node.server.manifests()])

    @app.get("/api/search")
    def search():
        try:
            return jsonify(results=dash.node.tracker.search(request.args.get("q", "")[:100]))
        except TrackerError as e:
            return jsonify(error=str(e)), 502

    @app.post("/api/downloads")
    def start():
        data = request.get_json(silent=True) or {}
        file_id = data.get("file_id")
        if not is_file_id(file_id):
            return jsonify(error="file_id must be 64 hex characters"), 400
        try:
            job = dash.start_download(file_id)
        except TrackerError as e:
            return jsonify(error=str(e)), 502
        return jsonify(job=job.to_dict()), 202

    @app.get("/api/downloads")
    def downloads():
        return jsonify(jobs=dash.job_list(), history=dash.history())

    @app.post("/api/rescan")
    def rescan():
        try:
            return jsonify(shared=dash.rescan())
        except TrackerError as e:
            return jsonify(error=str(e)), 502

    return app


def serve_dashboard(node: PeerNode, port: int, db_path: Path | None = None) -> str:
    """Start the dashboard in a background thread; return its URL."""
    dash = Dashboard(node, db_path or Path("data") / f"peer-{node.name}.db")
    server = make_server("127.0.0.1", port, create_dashboard_app(dash), threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True, name="dashboard").start()
    return f"http://127.0.0.1:{server.server_port}"