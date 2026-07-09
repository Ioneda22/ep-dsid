"""Divisão e reconstrução de arquivos em chunks de tamanho fixo.

O PeerSpot usa chunks de 256 KiB (262144 bytes) por padrão. O último chunk
pode ser menor — o tamanho total do arquivo não precisa ser múltiplo de
chunk_size.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator
from pathlib import Path

#: Tamanho default de chunk em bytes (256 KiB).
DEFAULT_CHUNK_SIZE = 262144


def chunk_count(file_size: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Calcula quantos chunks compõem um arquivo de tamanho file_size.

    Args:
        file_size: Tamanho total do arquivo em bytes (>= 0).
        chunk_size: Tamanho de cada chunk em bytes (> 0).

    Returns:
        Número de chunks. Arquivo vazio retorna 0.

    Raises:
        ValueError: Se file_size < 0 ou chunk_size <= 0.
    """
    if file_size < 0:
        raise ValueError(f"file_size deve ser >= 0; recebido {file_size}")
    if chunk_size <= 0:
        raise ValueError(f"chunk_size deve ser > 0; recebido {chunk_size}")
    if file_size == 0:
        return 0
    return math.ceil(file_size / chunk_size)


def split_file(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[bytes]:
    """Itera sobre os chunks de um arquivo, em ordem do início ao fim.

    Cada chunk tem exatamente chunk_size bytes, exceto possivelmente o
    último, que pode ser menor. O arquivo é lido sob demanda — adequado para
    arquivos grandes.

    Args:
        path: Caminho do arquivo a ser dividido.
        chunk_size: Tamanho de cada chunk em bytes.

    Yields:
        Bytes de cada chunk, em ordem.

    Raises:
        FileNotFoundError: Se path não existir.
        ValueError: Se chunk_size <= 0.
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size deve ser > 0; recebido {chunk_size}")
    with path.open("rb") as f:
        while True:
            buf = f.read(chunk_size)
            if not buf:
                break
            yield buf


def join_chunks(chunks: Iterable[bytes], destino: Path) -> int:
    """Reconstrói um arquivo a partir de seus chunks em ordem.

    Args:
        chunks: Iterável de blocos de bytes em ordem do chunk 0 ao N-1.
        destino: Caminho onde o arquivo reconstruído será escrito. Diretórios
            pais devem existir.

    Returns:
        Tamanho total escrito em bytes.

    Raises:
        OSError: Em falha de escrita.
    """
    total = 0
    with destino.open("wb") as f:
        for chunk in chunks:
            f.write(chunk)
            total += len(chunk)
    return total
