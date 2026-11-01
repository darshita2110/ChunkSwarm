import os
from pathlib import Path
from chunkswarm.manifest import build_manifest, sha256_file

p = Path("playground/test.bin")
p.write_bytes(os.urandom(2_500_000))        # a random 2.5 MB file

m = build_manifest(p)
print("file_id:   ", m.file_id)
print("size:      ", m.size)
print("chunks:    ", m.num_chunks)
for i, h in enumerate(m.chunk_hashes):
    print(f"  chunk {i}: {h[:16]}...")
print("matches sha256_file:", m.file_id == sha256_file(p))