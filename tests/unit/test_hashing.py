"""Testes unitários para src/common/hashing.py."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from src.common.hashing import sha256_bytes, sha256_file

# Vetor conhecido: sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
SHA256_VAZIO = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
SHA256_ABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_sha256_bytes_vazio() -> None:
    assert sha256_bytes(b"") == SHA256_VAZIO


def test_sha256_bytes_abc() -> None:
    assert sha256_bytes(b"abc") == SHA256_ABC


def test_sha256_file_eq_sha256_bytes(tmp_path: Path) -> None:
    data = b"PeerSpot test payload " * 1000  # ~22 KB
    f = tmp_path / "amostra.bin"
    f.write_bytes(data)
    assert sha256_file(f) == sha256_bytes(data)


def test_sha256_file_grande_streaming(tmp_path: Path) -> None:
    """Arquivo de ~3 MiB (maior que o buffer de 1 MiB) deve hashar correto."""
    data = os.urandom(3 * 1024 * 1024 + 17)
    f = tmp_path / "grande.bin"
    f.write_bytes(data)
    assert sha256_file(f) == hashlib.sha256(data).hexdigest()


def test_sha256_file_inexistente(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        sha256_file(tmp_path / "nao-existe.bin")
