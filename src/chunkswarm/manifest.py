"""ChunkSwarm manifests: a file's ID card.

Big idea 1 - content addressing
    A file's ID is the SHA-256 hash of its whole content.
    Same bytes  -> same ID, on every peer.  (So 3 peers sharing notes.pdf
    are automatically known to have the SAME file.)
    One bit different -> completely different ID.

Big idea 2 - per-chunk hashes
    The file is cut into chunks (1 MiB by default). The manifest stores one
    SHA-256 per chunk, so a bad chunk is caught the moment it arrives.

Example: a 2.5 MiB file with 1 MiB chunks
    chunk 0: bytes 0       .. 1 MiB       (1 MiB)
    chunk 1: bytes 1 MiB   .. 2 MiB       (1 MiB)
    chunk 2: bytes 2 MiB   .. 2.5 MiB     (0.5 MiB, the last one is shorter)
    manifest = {file_id, name, size, chunk_size, chunk_hashes: [h0, h1, h2]}

YOUR TASK: fill in TODOs A to E. Run `pytest tests/test_manifest.py -v`.
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CHUNK_SIZE = 1024 * 1024          # 1 MiB
MIN_CHUNK_SIZE = 64 * 1024                # 64 KiB
MAX_CHUNK_SIZE = 4 * 1024 * 1024          # 4 MiB
MAX_FILE_SIZE = 4 * 1024 ** 3             # 4 GiB per file

HEX64 = re.compile(r"^[0-9a-f]{64}$")     # what a SHA-256 hex string looks like


class ManifestError(ValueError):
    """A manifest is broken or was tampered with."""


def is_file_id(value: object) -> bool:
    """True only for a string of exactly 64 lowercase hex characters."""
    return isinstance(value, str) and bool(HEX64.match(value))


# ------------------------------------------------------------------ hashing
def sha256_file(path: Path) -> str:
    """SHA-256 of a whole file, as 64 hex characters.

    Read the file in 1 MiB blocks so a 4 GB file never sits in memory at once.
    """
    # TODO A
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def build_manifest(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> "Manifest":
    """Read a file ONCE and produce its manifest."""
    # TODO B
    path = Path(path)
    size = path.stat().st_size
    if size > MAX_FILE_SIZE:
        raise ManifestError(f"{path.name} is bigger than the {MAX_FILE_SIZE} byte limit")
    whole = hashlib.sha256()
    chunk_hashes = []
    with path.open("rb") as f:
        while block := f.read(chunk_size):
            whole.update(block)
            chunk_hashes.append(hashlib.sha256(block).hexdigest())
    return Manifest(whole.hexdigest(), safe_filename(path.name), size, chunk_size,
                    tuple(chunk_hashes))


# ----------------------------------------------------------- safe file names
def safe_filename(name: str, fallback: str = "download.bin") -> str:
    """Turn a name sent by an untrusted peer into a harmless local file name.

    '../../etc/passwd'  -> 'passwd'      (no escaping our download folder)
    '.hidden'           -> 'hidden'
    'a\\x00b\\nc.txt'     -> 'abc.txt'     (no invisible control characters)
    '..'                -> 'download.bin'
    """
    # TODO D
    if not isinstance(name, str):
        return fallback
    name = unicodedata.normalize("NFC", name)
    name = name.replace("\\", "/").split("/")[-1]
    name = "".join(c for c in name if c.isprintable() and c not in '<>:"|?*')
    name = name.strip().lstrip(".").strip()
    if len(name) > 200:
        stem, dot, ext = name.rpartition(".")
        name = (stem[:190] + dot + ext[:9]) if dot and len(ext) <= 9 else name[:200]
    return name or fallback


# ------------------------------------------------------------------ manifest
@dataclass(frozen=True)          # frozen = can't be changed after creation
class Manifest:
    file_id: str                 # SHA-256 of the whole file
    name: str                    # file name (already made safe)
    size: int                    # total bytes
    chunk_size: int              # bytes per chunk (last one may be shorter)
    chunk_hashes: tuple[str, ...] = field(repr=False)   # one SHA-256 per chunk

    @property
    def num_chunks(self) -> int:
        return len(self.chunk_hashes)

    def chunk_span(self, index: int) -> tuple[int, int]:
        """Where chunk `index` lives in the file: (offset, length)."""
        # TODO C
        if not 0 <= index < self.num_chunks:
            raise IndexError(index)
        offset = index * self.chunk_size
        return offset, min(self.chunk_size, self.size - offset)

    def to_dict(self) -> dict:
        """Plain dict, ready for json.dumps (to send to the tracker or a peer)."""
        return {
            "file_id": self.file_id,
            "name": self.name,
            "size": self.size,
            "chunk_size": self.chunk_size,
            "chunk_hashes": list(self.chunk_hashes),
        }

    @classmethod
    def from_dict(cls, data: object) -> "Manifest":
        """Build a Manifest from UNTRUSTED data, checking every field."""
        # TODO E
        if not isinstance(data, dict):
            raise ManifestError("manifest must be an object")
        file_id = data.get("file_id")
        name = data.get("name")
        size = data.get("size")
        chunk_size = data.get("chunk_size")
        hashes = data.get("chunk_hashes")

        if not is_file_id(file_id):
            raise ManifestError("file_id must be 64 lowercase hex chars")
        if not isinstance(name, str) or not name or len(name) > 255:
            raise ManifestError("invalid name")
        if type(size) is not int or not 0 <= size <= MAX_FILE_SIZE:
            raise ManifestError("invalid size")
        if type(chunk_size) is not int or not MIN_CHUNK_SIZE <= chunk_size <= MAX_CHUNK_SIZE:
            raise ManifestError("invalid chunk_size")
        if not isinstance(hashes, list) or len(hashes) != math.ceil(size / chunk_size):
            raise ManifestError("chunk_hashes count does not match size/chunk_size")
        if not all(is_file_id(h) for h in hashes):
            raise ManifestError("chunk hashes must be 64 lowercase hex chars")
        return cls(file_id, safe_filename(name), size, chunk_size, tuple(hashes))