"""ChunkSwarm peer server: the part of a peer that SENDS chunks to others.

How it works
------------
    another peer                         this server
        |  -- TCP connect ------------------>  accept()  -> new thread for this client
        |  -- GET_CHUNK {file_id, index:0} ->  look up file_id, read chunk 0 from disk
        |  <- CHUNK_DATA {index:0} + 1 MB ---
        |  -- GET_CHUNK {file_id, index:1} ->  (same connection, next request)
        |  <- CHUNK_DATA {index:1} + 1 MB ---
        |  -- close ------------------------>  thread ends

Safety rules
------------
1. Peers ask by file_id (a SHA-256), NEVER by path. We only know ids we
   registered ourselves, so "../../etc/passwd" can never reach the disk.
2. Only files inside share_dir can be shared (shortcuts are followed first).
3. Limits: max connections at once, and silent clients get disconnected.

YOUR TASK: fill in TODOs A to D. Run `pytest tests/test_peer_server.py -v`.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from pathlib import Path

from .manifest import DEFAULT_CHUNK_SIZE, Manifest, build_manifest, is_file_id
from .protocol import (ConnectionClosed, MsgType, ProtocolError, recv_msg,
                       send_error, send_msg)

log = logging.getLogger("chunkswarm.server")

def _close_gracefully(conn: socket.socket) -> None:
    """Hang up politely so our last message is not lost."""
    try:
        conn.shutdown(socket.SHUT_WR)
        conn.settimeout(1.0)
        drained = 0
        while drained < 1024 * 1024:
            data = conn.recv(65536)
            if not data:
                break
            drained += len(data)
    except OSError:
        pass

class PeerServer:
    def __init__(self, share_dir: Path, host: str = "127.0.0.1", port: int = 0, *,
                 chunk_size: int = DEFAULT_CHUNK_SIZE, max_connections: int = 32,
                 idle_timeout: float = 30.0, upload_limit: float = 0):
        self.share_dir = Path(share_dir).resolve()   # full real path, e.g. D:\apps\...\shared
        self.share_dir.mkdir(parents=True, exist_ok=True)
        self.host = host
        self.port = port                 # 0 = "OS, pick any free port for me"
        self.chunk_size = chunk_size
        self.idle_timeout = idle_timeout
        self.upload_limit = upload_limit
        # A counter of free "seats". acquire() takes one, release() gives it back.
        self._slots = threading.BoundedSemaphore(max_connections)
        # file_id -> (path on disk, manifest). Only files in here can be served.
        self._files: dict[str, tuple[Path, Manifest]] = {}
        self._lock = threading.Lock()    # many threads read _files at once
        self._sock: socket.socket | None = None
        self._stopping = threading.Event()
        self.stats = {"connections": 0, "chunks_served": 0, "bytes_served": 0, "rejected": 0}

    # ================================================================ sharing
    def share(self, path: Path) -> Manifest:
        """Register one file for sharing and return its manifest.

        Must raise PermissionError if the file is outside share_dir
        (including a shortcut/symlink that POINTS outside).
        """
        # TODO A
        resolved = Path(path).resolve()
        if not resolved.is_relative_to(self.share_dir):
            raise PermissionError(f"{path} is outside the share folder")
        if not resolved.is_file():
            raise FileNotFoundError(path)
        manifest = build_manifest(resolved, self.chunk_size)
        with self._lock:
            self._files[manifest.file_id] = (resolved, manifest)
        return manifest

    def share_all(self) -> list[Manifest]:
        """Share every normal file inside share_dir (skips hidden files)."""
        out = []
        for p in sorted(self.share_dir.rglob("*")):     # rglob = all files, all subfolders
            if p.is_file() and not p.name.startswith("."):
                try:
                    out.append(self.share(p))
                except (PermissionError, OSError) as e:
                    log.warning("skipping %s: %s", p, e)
        return out

    def manifests(self) -> list[Manifest]:
        with self._lock:
            return [m for _, m in self._files.values()]

    def _lookup(self, file_id: object) -> tuple[Path, Manifest] | None:
        """Find a shared file by id. Returns None for anything not shared."""
        if not is_file_id(file_id):
            return None
        with self._lock:
            return self._files.get(file_id)

    def _read_chunk(self, path: Path, manifest: Manifest, index: int) -> bytes:
        """Read exactly chunk `index` from disk."""
        # TODO B
    def _read_chunk(self, path: Path, manifest: Manifest, index: int) -> bytes:
        """Read exactly chunk `index` from disk."""
        offset, length = manifest.chunk_span(index)
        with path.open("rb") as f:
            f.seek(offset)
            data = f.read(length)
        if len(data) != length:
            raise OSError("file changed on disk since it was shared")
        return data

    # ============================================================== lifecycle
    def start(self) -> "PeerServer":
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(64)
        sock.settimeout(0.5)              # accept() wakes up twice a second to check stop()
        self._sock = sock
        self.port = sock.getsockname()[1]  # the real port, if we asked for 0
        threading.Thread(target=self._accept_loop, name="accept", daemon=True).start()
        log.info("peer server listening on %s:%d", self.host, self.port)
        return self

    def stop(self) -> None:
        self._stopping.set()
        if self._sock:
            self._sock.close()

    def _accept_loop(self) -> None:
        """Wait for clients forever; give each one its own thread."""
        while not self._stopping.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue                  # nobody came in 0.5 s; check stop() and wait again
            except OSError:
                break                     # the socket was closed by stop()
            if not self._slots.acquire(blocking=False):   # no free seat?
                self.stats["rejected"] += 1
                conn.settimeout(1.0)
                send_error(conn, "BUSY", "too many connections")
                conn.close()
                continue
            self.stats["connections"] += 1
            threading.Thread(target=self._serve, args=(conn, addr), daemon=True).start()

    # ============================================================= one client
    def _serve(self, conn: socket.socket, addr) -> None:
        """Handle one connected client until it leaves. Runs in its own thread."""
        # TODO C
        conn.settimeout(self.idle_timeout)
        try:
            while not self._stopping.is_set():
                try:
                    msg_type, header, _ = recv_msg(conn, max_payload=0)
                except ConnectionClosed:
                    return
                except ProtocolError as e:
                    self.stats["rejected"] += 1
                    send_error(conn, "PROTOCOL", str(e))
                    _close_gracefully(conn)
                    return
                if not self._dispatch(conn, msg_type, header):
                    return
        except (socket.timeout, ConnectionError, OSError) as e:
            log.debug("connection %s ended: %s", addr, e)
        finally:
            conn.close()
            self._slots.release()

    def _dispatch(self, conn: socket.socket, msg_type: MsgType, header: dict) -> bool:
        """Answer one request. Return True to keep the connection, False to close it."""
        # TODO D
        if msg_type is MsgType.PING:
            send_msg(conn, MsgType.PONG)
            return True

        if msg_type is MsgType.GET_MANIFEST:
            entry = self._lookup(header.get("file_id"))
            if entry is None:
                self.stats["rejected"] += 1
                send_error(conn, "NOT_FOUND", "file not shared by this peer")
                return True
            send_msg(conn, MsgType.MANIFEST, {"manifest": entry[1].to_dict()})
            return True

        if msg_type is MsgType.GET_CHUNK:
            entry = self._lookup(header.get("file_id"))
            if entry is None:
                self.stats["rejected"] += 1
                send_error(conn, "NOT_FOUND", "file not shared by this peer")
                return True
            path, manifest = entry
            index = header.get("index")
            if type(index) is not int or not 0 <= index < manifest.num_chunks:
                self.stats["rejected"] += 1
                send_error(conn, "BAD_INDEX", "chunk index out of range")
                return True
            try:
                data = self._read_chunk(path, manifest, index)
            except OSError:
                send_error(conn, "UNAVAILABLE", "chunk could not be read")
                return True
            send_msg(conn, MsgType.CHUNK_DATA,
                     {"file_id": manifest.file_id, "index": index, "size": len(data)}, data)
            self.stats["chunks_served"] += 1
            self.stats["bytes_served"] += len(data)
            if self.upload_limit:
                time.sleep(len(data) / self.upload_limit)
            return True

        self.stats["rejected"] += 1
        send_error(conn, "BAD_REQUEST", f"unexpected message {msg_type.name}")
        return False