import os
from pathlib import Path
from chunkswarm.peer_server import PeerServer

share = Path("playground/shared")
share.mkdir(exist_ok=True)
(share / "photo.bin").write_bytes(os.urandom(2_500_000))   # 2.5 MB test file

srv = PeerServer(share)
m = srv.share(share / "photo.bin")
print("shared:", m.name, "| chunks:", m.num_chunks, "| id:", m.file_id[:16], "...")

path, manifest = srv._lookup(m.file_id)
for i in range(manifest.num_chunks):
    print(f"chunk {i}: {len(srv._read_chunk(path, manifest, i))} bytes")

try:
    srv.share(Path("README.md"))          # a file OUTSIDE the shared folder
except PermissionError as e:
    print("blocked:", e)