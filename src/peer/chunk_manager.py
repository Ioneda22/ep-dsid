"""Rastreamento de progresso de download."""

from __future__ import annotations


class ChunkManager:
    """Rastreia quais chunks foram baixados para cada arquivo."""

    def __init__(self) -> None:
        """Inicializa rastreador de chunks."""
        self.downloads: dict[str, set[int]] = {}

    def start_download(self, hash_arquivo: str, n_chunks: int) -> None:
        """
        Inicia rastreamento de download para um arquivo.

        Args:
            hash_arquivo: Hash do arquivo.
            n_chunks: Número total de chunks esperados.
        """
        if hash_arquivo not in self.downloads:
            self.downloads[hash_arquivo] = set()

    def mark_received(self, hash_arquivo: str, chunk_index: int) -> None:
        """
        Marca um chunk como recebido.

        Args:
            hash_arquivo: Hash do arquivo.
            chunk_index: Índice do chunk.
        """
        if hash_arquivo not in self.downloads:
            self.downloads[hash_arquivo] = set()
        self.downloads[hash_arquivo].add(chunk_index)

    def missing_chunks(self, hash_arquivo: str, n_chunks: int) -> list[int]:
        """
        Retorna lista de chunks faltando.

        Args:
            hash_arquivo: Hash do arquivo.
            n_chunks: Número total de chunks.

        Returns:
            Lista de índices de chunks faltando (0 a n_chunks-1).
        """
        if hash_arquivo not in self.downloads:
            return list(range(n_chunks))

        received = self.downloads[hash_arquivo]
        return [i for i in range(n_chunks) if i not in received]

    def is_complete(self, hash_arquivo: str, n_chunks: int) -> bool:
        """
        Verifica se um arquivo está completo.

        Args:
            hash_arquivo: Hash do arquivo.
            n_chunks: Número total de chunks.

        Returns:
            True se todos os chunks foram recebidos.
        """
        if hash_arquivo not in self.downloads:
            return False
        return len(self.downloads[hash_arquivo]) == n_chunks

    def progress(self, hash_arquivo: str, n_chunks: int) -> tuple[int, int]:
        """
        Retorna progresso do download.

        Args:
            hash_arquivo: Hash do arquivo.
            n_chunks: Número total de chunks.

        Returns:
            Tuple (chunks_recebidos, total_chunks).
        """
        if hash_arquivo not in self.downloads:
            return (0, n_chunks)
        return (len(self.downloads[hash_arquivo]), n_chunks)

    def reset(self, hash_arquivo: str) -> None:
        """Limpa progresso de um arquivo."""
        if hash_arquivo in self.downloads:
            del self.downloads[hash_arquivo]
