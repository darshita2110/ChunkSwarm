import hashlib
import os

import pytest

from chunkswarm.manifest import (MIN_CHUNK_SIZE, Manifest, ManifestError, build_manifest,
                          safe_filename)

CS = MIN_CHUNK_SIZE


def test_manifest_hashes_and_partial_last_chunk(tmp_path):
    data = os.urandom(CS * 3 + 123)
    p = tmp_path / "f.bin"
    p.write_bytes(data)
    m = build_manifest(p, CS)
    assert m.file_id == hashlib.sha256(data).hexdigest()
    assert m.num_chunks == 4
    assert m.chunk_span(3) == (CS * 3, 123)
    assert m.chunk_hashes[1] == hashlib.sha256(data[CS:2 * CS]).hexdigest()
    assert Manifest.from_dict(m.to_dict()) == m


def test_empty_file(tmp_path):
    p = tmp_path / "empty"
    p.write_bytes(b"")
    m = build_manifest(p, CS)
    assert m.num_chunks == 0 and m.size == 0


@pytest.mark.parametrize("raw,expected", [
    ("../../etc/passwd", "passwd"),
    ("/abs/path/x.txt", "x.txt"),
    ("..\\..\\windows\\evil.exe", "evil.exe"),
    ("..", "download.bin"),
    (".hidden", "hidden"),
    ("a\x00b\nc.txt", "abc.txt"),
    ('<script>alert(1)</script>.pdf', "script.pdf"),
])
def test_safe_filename(raw, expected):
    assert safe_filename(raw) == expected


def _valid(tmp_path):
    p = tmp_path / "f"
    p.write_bytes(os.urandom(CS + 5))
    return build_manifest(p, CS).to_dict()


@pytest.mark.parametrize("mutate", [
    lambda d: d.update(file_id="../../etc/passwd"),
    lambda d: d.update(file_id="A" * 64),           # uppercase not allowed
    lambda d: d.update(size=-1),
    lambda d: d.update(size=True),                  # bool is not an int here
    lambda d: d.update(chunk_size=1),
    lambda d: d["chunk_hashes"].pop(),              # count mismatch
    lambda d: d["chunk_hashes"].__setitem__(0, "zz"),
    lambda d: d.update(name=""),
])
def test_from_dict_rejects_bad_manifests(tmp_path, mutate):
    d = _valid(tmp_path)
    mutate(d)
    with pytest.raises(ManifestError):
        Manifest.from_dict(d)


def test_from_dict_sanitises_name(tmp_path):
    d = _valid(tmp_path)
    d["name"] = "../../../.bashrc"
    assert Manifest.from_dict(d).name == "bashrc"