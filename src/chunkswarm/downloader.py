"""ChunkSwarm downloader: the part of a peer that RECEIVES a file.

Trust rules
-----------
- The manifest (file ID card) comes from the tracker, and its file_id must
  equal the id we asked for.
- Other peers are NOT trusted. Every chunk is fingerprinted (SHA-256) and
  compared with the manifest BEFORE we keep it.
- If a peer sends a bad chunk, an error, goes silent or disconnects, we drop
  that peer and ask the NEXT peer for the SAME chunk (failover).
- At the end the whole file must hash to file_id, or we throw it away.

How the file is written
-----------------------
    downloads/<file_id>.part      <- chunks are written here, each at its own
                                     position (chunk 2 -> byte 2 MiB)
    downloads/movie.bin           <- renamed only after the final check passes

YOUR TASK: fill in TODOs A, B, C. Run `pytest tests/test_downloader.py -v`.
"""

from __future__ import annotations
import json
import os
import hashlib
import ipaddress
import logging
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

from .manifest import Manifest, safe_filename, sha256_file
from .protocol import MsgType, recv_msg, send_msg

log = logging.getLogger("chunkswarm.download")


class DownloadError(Exception):
    """The download could not be completed."""


class IntegrityError(DownloadError):
    """Data did not match its fingerprint."""


# ====================================================================== sources
@dataclass(frozen=True)
class Source:
    """One peer we can download from."""
    host: str
    port: int
    peer_id: str = ""

    @classmethod
    def validated(cls, host: object, port: object, peer_id: object = "") -> "Source":
        """Build a Source from UNTRUSTED data (the tracker's reply).

        Accept only a real IP address (not a name like evil.example.com),
        not 0.0.0.0 and not a multicast address, and a port that is an
        int from 1 to 65535. Otherwise raise DownloadError.
        """
        # TODO A
        try:
            ip = ipaddress.ip_address(str(host))
        except ValueError:
            raise DownloadError(f"invalid source host {host!r}") from None
        if ip.is_multicast or ip.is_unspecified:
            raise DownloadError(f"refusing source address {ip}")
        if type(port) is not int or not 1 <= port <= 65535:
            raise DownloadError(f"invalid source port {port!r}")
        return cls(str(ip), port, str(peer_id))


# ====================================================================== result
@dataclass
class DownloadResult:
    """What happened during one download (also our benchmark numbers later)."""
    path: Path                                   # where the finished file is
    size: int                                    # bytes
    seconds: float                               # how long it took
    chunks_by_source: dict[str, int] = field(default_factory=dict)   # "ip:port" -> chunks
    failed_sources: dict[str, str] = field(default_factory=dict)     # "ip:port" -> why
    resumed_chunks: int = 0   
                       # chunks kept from an earlier attempt
    @property
    def mb_per_s(self) -> float:
        return (self.size / 1e6) / self.seconds if self.seconds > 0 else float("inf")


# ====================================================================== one connection
class _PeerConn:
    """One open TCP connection to one peer."""

    def __init__(self, source: Source, connect_timeout: float, io_timeout: float):
        self.source = source
        self.sock = socket.create_connection((source.host, source.port), timeout=connect_timeout)
        self.sock.settimeout(io_timeout)   # a peer silent for this long counts as dead

    def get_chunk(self, manifest: Manifest, index: int) -> bytearray:
        """Ask for chunk `index` and return it ONLY if it is exactly right.

        Raise DownloadError if the peer replies with an ERROR, the wrong message,
        or a different chunk. Raise IntegrityError if the size or SHA-256 is wrong.
        """
        # TODO B
        _, expected_len = manifest.chunk_span(index)
        send_msg(self.sock, MsgType.GET_CHUNK, {"file_id": manifest.file_id, "index": index})
        msg_type, header, payload = recv_msg(self.sock, max_payload=manifest.chunk_size)
        if msg_type is MsgType.ERROR:
            raise DownloadError(f"peer error {header.get('code')}: {header.get('message')}")
        if msg_type is not MsgType.CHUNK_DATA:
            raise DownloadError(f"unexpected reply {msg_type.name}")
        if header.get("file_id") != manifest.file_id or header.get("index") != index:
            raise DownloadError("reply is for a different chunk")
        if len(payload) != expected_len:
            raise IntegrityError(f"chunk {index}: expected {expected_len} bytes, got {len(payload)}")
        if hashlib.sha256(payload).hexdigest() != manifest.chunk_hashes[index]:
            raise IntegrityError(f"chunk {index}: hash mismatch")
        return payload

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


# ====================================================================== helpers
def _unique_path(directory: Path, name: str) -> Path:
    """movie.bin -> 'movie (1).bin' if movie.bin already exists (never overwrite)."""
    candidate = directory / name
    stem, suffix = Path(name).stem, Path(name).suffix
    n = 1
    while candidate.exists():
        candidate = directory / f"{stem} ({n}){suffix}"
        n += 1
    return candidate

def _state_path(download_dir: Path, file_id: str) -> Path:
    return download_dir / f"{file_id}.state.json"


def _save_state(state_path: Path, manifest: Manifest, done: set[int]) -> None:
    """Remember which chunks are verified, safely even if we crash mid-write."""
    data = {"file_id": manifest.file_id, "chunk_size": manifest.chunk_size,
            "done": sorted(done)}
    tmp = state_path.with_name(state_path.name + ".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, state_path)


def _load_state(part_path: Path, state_path: Path, manifest: Manifest) -> set[int]:
    """Which chunks of an earlier attempt are still good? Empty set = start fresh."""
    if not part_path.exists() or not state_path.exists():
        return set()
    try:
        data = json.loads(state_path.read_text())
    except (OSError, ValueError):
        return set()
    if (not isinstance(data, dict) or data.get("file_id") != manifest.file_id
            or data.get("chunk_size") != manifest.chunk_size
            or part_path.stat().st_size != manifest.size
            or not isinstance(data.get("done"), list)):
        return set()
    done = set()
    with part_path.open("rb") as f:
        for index in data["done"]:
            if type(index) is not int or not 0 <= index < manifest.num_chunks:
                continue
            offset, length = manifest.chunk_span(index)
            f.seek(offset)
            if hashlib.sha256(f.read(length)).hexdigest() == manifest.chunk_hashes[index]:
                done.add(index)
    return done

ProgressFn = Callable[[int, int, str], None]   # (chunks_done, total_chunks, "ip:port")


# ====================================================================== the download
def download(manifest: Manifest, sources: Iterable[Source], download_dir: Path, *,
             expected_file_id: str | None = None, connect_timeout: float = 5.0,
             io_timeout: float = 15.0, progress: ProgressFn | None = None) -> DownloadResult:
    """Download the file described by `manifest` from `sources`, verified."""
    # TODO C
## heart of the downloader

    #the setup and safety checks
    if expected_file_id is not None and manifest.file_id != expected_file_id:
        raise IntegrityError("manifest does not describe the requested file")
    sources = list(dict.fromkeys(sources))
    if not sources:
        raise DownloadError("no sources available")

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    part_path = download_dir / f"{manifest.file_id}.part"
    state_path = _state_path(download_dir, manifest.file_id)
    done = _load_state(part_path, state_path, manifest)
    result = DownloadResult(path=part_path, size=manifest.size, seconds=0.0,
                            resumed_chunks=len(done))
    start = time.perf_counter()
    conn = None
    remaining = list(sources)

    #the main loop
    try:
        mode = "r+b" if done else "wb"
        with part_path.open(mode) as out:
            if not done:
                out.truncate(manifest.size)
            for index in range(manifest.num_chunks):
                if index in done:
                    continue
                while True:
                    if conn is None:
                        if not remaining:
                            raise DownloadError(
                                f"all sources failed at chunk {index}: {result.failed_sources}")
                        src = remaining[0]
                        try:
                            conn = _PeerConn(src, connect_timeout, io_timeout)
                        except OSError as e:
                            result.failed_sources[f"{src.host}:{src.port}"] = f"connect: {e}"
                            remaining.pop(0)
                            continue
                    label = f"{conn.source.host}:{conn.source.port}"
                    try:
                        data = conn.get_chunk(manifest, index)
                    except (DownloadError, OSError) as e:
                        log.warning("source %s failed on chunk %d: %s", label, index, e)
                        result.failed_sources[label] = str(e)
                        conn.close()
                        conn = None
                        remaining.pop(0)
                        continue
                    offset, _ = manifest.chunk_span(index)
                    out.seek(offset)
                    out.write(data)
                    done.add(index)
                    _save_state(state_path, manifest, done)
                    result.chunks_by_source[label] = result.chunks_by_source.get(label, 0) + 1
                    if progress:
                        progress(len(done), manifest.num_chunks, label)
                    break
    # cleanup if anything goes wrong
    finally:
        if conn:
            conn.close()

    #Final check and rename
    if sha256_file(part_path) != manifest.file_id:
        part_path.unlink(missing_ok=True)
        state_path.unlink(missing_ok=True)
        raise IntegrityError("final file hash does not match file_id")

    final_path = _unique_path(download_dir, safe_filename(manifest.name))
    part_path.replace(final_path)
    state_path.unlink(missing_ok=True)
    result.path = final_path
    result.seconds = time.perf_counter() - start
    return result