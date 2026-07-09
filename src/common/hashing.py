"""Funções de hashing SHA-256 utilizadas como identificador de arquivos.

O identificador de cada arquivo no PeerSpot é o SHA-256 hex do conteúdo
completo (não dos chunks individuais).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

#: Tamanho do bloco usado por sha256_file ao ler do disco.
_READ_BUFFER_BYTES = 1024 * 1024  # 1 MiB


def sha256_bytes(data: bytes) -> str:
    """Retorna o SHA-256 hexadecimal de uma sequência de bytes.

    Args:
        data: Bytes a serem hasheados.

    Returns:
        Hash SHA-256 em hexadecimal minúsculo (64 caracteres).
    """
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    """Calcula o SHA-256 hexadecimal de um arquivo em modo streaming.

    Lê o arquivo em blocos de 1 MiB para não carregar tudo em memória, o que
    é essencial para arquivos de áudio grandes.

    Args:
        path: Caminho do arquivo a ser hasheado.

    Returns:
        Hash SHA-256 em hexadecimal minúsculo (64 caracteres).

    Raises:
        FileNotFoundError: Se path não existir.
        OSError: Em falha de leitura.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(_READ_BUFFER_BYTES)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()
