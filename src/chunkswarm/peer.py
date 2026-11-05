"""ChunkSwarm peer app: everything you built, joined into one program.

    +---------------------------- one peer ----------------------------+
    |  PeerServer (Step 5)    sends chunks to other peers   (TCP)       |
    |  download() (Step 6)    gets chunks from other peers  (TCP)       |
    |  TrackerClient (here)   talks to the tracker          (HTTP)      |
    |  heartbeat thread       "still here" every few seconds            |
    |  REPL                   the commands you type: search, get, ...   |
    +-------------------------------------------------------------------+

Run one per computer (or several on one computer with different ports/folders):

    chunkswarm-peer --share-dir shared_a --download-dir dl_a --port 9001 --name A

YOUR TASK: TODOs A1, A2, B, C. Run `pytest tests/test_end_to_end.py -v`.
"""

from __future__ import annotations

import argparse
import logging
import shlex
import threading
from pathlib import Path

import requests

from .downloader import DownloadResult, Source, download
from .manifest import Manifest, is_file_id
from .peer_server import PeerServer

log = logging.getLogger("chunkswarm.peer")


class TrackerError(Exception):
    pass


class TrackerClient:
    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.peer_id: str | None = None
        self.chunk_size: int | None = None
        self.heartbeat_interval = 10.0

    def _call(self, method: str, path: str, **kw) -> dict:
        """Send one HTTP request to the tracker and return its JSON reply.

        Raise TrackerError if the tracker is unreachable, replies with
        something that is not JSON, or replies with an error status.
        """
        # TODO A1
        try:
            r = self.session.request(method, self.base_url + path, timeout=self.timeout, **kw)
        except requests.RequestException as e:
            raise TrackerError(f"tracker unreachable: {e}") from None
        try:
            data = r.json()
        except ValueError:
            raise TrackerError(f"tracker returned non-JSON (HTTP {r.status_code})") from None
        if not r.ok:
            raise TrackerError(f"HTTP {r.status_code}: {data.get('error', 'error')}")
        return data

    def register(self, name: str, port: int) -> None:
        """Register, remember our id + chunk size, and attach the token to
        every later request."""
        # TODO A2
        data = self._call("POST", "/api/peers/register", json={"name": name, "port": port})
        self.peer_id = data["peer_id"]
        self.chunk_size = data["chunk_size"]
        self.heartbeat_interval = float(data["heartbeat_interval"])
        self.session.headers["Authorization"] = f"Bearer {data['token']}"

    def heartbeat(self) -> None:
        self._call("POST", "/api/peers/heartbeat")

    def unregister(self) -> None:
        self._call("POST", "/api/peers/unregister")

    def share(self, manifest: Manifest) -> None:
        self._call("POST", "/api/files/share", json={"manifest": manifest.to_dict()})

    def search(self, query: str) -> list[dict]:
        return self._call("GET", "/api/files/search", params={"q": query})["results"]

    def manifest(self, file_id: str) -> Manifest:
        return Manifest.from_dict(self._call("GET", f"/api/files/{file_id}/manifest")["manifest"])

    def sources(self, file_id: str) -> list[Source]:
        out = []
        for s in self._call("GET", f"/api/files/{file_id}/sources")["sources"]:
            try:
                out.append(Source.validated(s.get("host"), s.get("port"), s.get("peer_id", "")))
            except Exception as e:  # untrusted tracker data: skip bad entries
                log.warning("ignoring invalid source %r: %s", s, e)
        return out


class PeerNode:
    def __init__(self, tracker_url: str, share_dir: Path, download_dir: Path, *,
                 host: str = "127.0.0.1", port: int = 0, name: str = "peer"):
        self.tracker = TrackerClient(tracker_url)
        self.share_dir = Path(share_dir)
        self.download_dir = Path(download_dir)
        self.host, self.port, self.name = host, port, name
        self.server: PeerServer | None = None
        self._stop = threading.Event()

    def start(self) -> "PeerNode":
        """Start the file server, register, share our files, start heartbeats."""
        # TODO B
        self.server = PeerServer(self.share_dir, self.host, self.port)
        self.server.start()
        self.tracker.register(self.name, self.server.port)
        self.server.chunk_size = self.tracker.chunk_size
        for m in self.server.share_all():
            self.tracker.share(m)
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        return self

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.tracker.heartbeat_interval):
            try:
                self.tracker.heartbeat()
            except TrackerError as e:
                log.warning("heartbeat failed: %s", e)

    def share_file(self, path: Path) -> Manifest:
        m = self.server.share(path)
        self.tracker.share(m)
        return m

    def get(self, file_id: str, progress=None) -> DownloadResult:
        """Download one file by its id: manifest + sources from the tracker, then download()."""
        # TODO C
        if not is_file_id(file_id):
            raise ValueError("file id must be 64 hex characters")
        manifest = self.tracker.manifest(file_id)
        sources = self.tracker.sources(file_id)
        return download(manifest, sources, self.download_dir,
                        expected_file_id=file_id, progress=progress)

    def stop(self) -> None:
        self._stop.set()
        try:
            self.tracker.unregister()
        except TrackerError:
            pass
        if self.server:
            self.server.stop()


HELP = """commands:
  search <text>     find files shared by other peers
  get <n|file_id>   download result n from the last search (or a full file id)
  share <path>      share another file (must be inside your share dir)
  files             list files you are sharing
  stats             server statistics
  quit"""


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def repl(node: PeerNode) -> None:
    last: list[dict] = []
    print(HELP)
    while True:
        try:
            line = input(f"{node.name}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        cmd, *args = shlex.split(line)
        try:
            if cmd in ("quit", "exit"):
                return
            elif cmd == "search":
                last = node.tracker.search(" ".join(args))
                if not last:
                    print("no results")
                for i, r in enumerate(last, 1):
                    print(f"  [{i}] {r['name']}  {_fmt_size(r['size'])}  "
                          f"{r['sources']} source(s)  {r['file_id'][:12]}…")
            elif cmd == "get" and args:
                target = args[0]
                file_id = last[int(target) - 1]["file_id"] if target.isdigit() else target

                shown = [-1]

                def bar(done, total, src):
                    pct = done * 100 // max(total, 1)
                    if pct // 4 == shown[0] and done != total:
                        return  # redraw only when the bar actually grows
                    shown[0] = pct // 4
                    print(f"\r  [{'#' * (pct // 4):<25}] {pct:3d}%  from {src}   ", end="", flush=True)

                res = node.get(file_id, progress=bar)
                print(f"\n  saved {res.path}  ({_fmt_size(res.size)} in {res.seconds:.2f}s, "
                      f"{res.mb_per_s:.1f} MB/s)  SHA-256 verified")
                if res.failed_sources:
                    print(f"  failed over from: {res.failed_sources}")
            elif cmd == "share" and args:
                m = node.share_file(Path(args[0]))
                print(f"  sharing {m.name} ({m.file_id[:12]}…)")
            elif cmd == "files":
                for m in node.server.manifests():
                    print(f"  {m.name}  {_fmt_size(m.size)}  {m.num_chunks} chunks  {m.file_id[:12]}…")
            elif cmd == "stats":
                print(f"  {node.server.stats}")
            else:
                print(HELP)
        except Exception as e:  # keep the REPL alive on any single failure
            print(f"  error: {e}")


def main() -> None:
    ap = argparse.ArgumentParser(description="P2P peer")
    ap.add_argument("--tracker", default="http://127.0.0.1:8000")
    ap.add_argument("--share-dir", default="shared_files")
    ap.add_argument("--download-dir", default="downloads")
    ap.add_argument("--host", default="127.0.0.1",
                    help="interface for the file server; use your LAN IP for multi-machine runs")
    ap.add_argument("--port", type=int, default=0, help="0 = pick a free port")
    ap.add_argument("--name", default="peer")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    node = PeerNode(args.tracker, Path(args.share_dir), Path(args.download_dir),
                    host=args.host, port=args.port, name=args.name).start()
    print(f"peer '{node.name}' serving on {node.host}:{node.server.port}, "
          f"sharing {len(node.server.manifests())} file(s)")
    try:
        repl(node)
    finally:
        node.stop()


if __name__ == "__main__":
    main()