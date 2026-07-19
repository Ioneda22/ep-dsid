"""Armazenamento local do peer: chunks em download e arquivos montados.

Layout em disco (storage_dir = data/<peer> do YAML):

    <hash>/chunks/chunk_<i>.bin   # chunks recebidos durante um download
    <hash>/arquivo                # arquivo completo montado

Para não duplicar espaço em disco, ao concluir um download os chunks
individuais são apagados e o arquivo montado vira a única cópia. Ao
servir outros peers, Storage.load_chunk extrai a fatia
correspondente do arquivo montado on-the-fly (seek/read) —
"repartir na hora de fornecer" em vez de manter as duas formas.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from src.common.chunking import DEFAULT_CHUNK_SIZE, chunk_count
from src.common.errors import InvalidHashError
from src.common.hashing import sha256_file


class Storage:
    """Gerencia chunks e arquivos completos no disco local do peer.

    Não é thread-safe por si só; cada chunk é gravado em um arquivo
    próprio (índices distintos não colidem) e o TCP server apenas lê
    arquivos já completos, o que não conflita.
    """

    def __init__(
        self, storage_dir: str | Path, chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> None:
        """Cria o storage ancorado em storage_dir (criado se ausente).

        Args:
            storage_dir: Diretório raiz (storage_dir do YAML).
            chunk_size: Tamanho do chunk em bytes (256 KiB).
        """
        self.storage_dir = Path(storage_dir)
        self.chunk_size = chunk_size
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Caminhos
    # ------------------------------------------------------------------

    def assembled_path(self, hash_arquivo: str) -> Path:
        """Caminho do arquivo completo montado para hash_arquivo."""
        return self.storage_dir / hash_arquivo / "arquivo"

    def _chunks_dir(self, hash_arquivo: str) -> Path:
        return self.storage_dir / hash_arquivo / "chunks"

    def _chunk_path(self, hash_arquivo: str, index: int) -> Path:
        return self._chunks_dir(hash_arquivo) / f"chunk_{index}.bin"

    # ------------------------------------------------------------------
    # Chunks (durante download e ao servir outros peers)
    # ------------------------------------------------------------------

    def save_chunk(self, hash_arquivo: str, index: int, data: bytes) -> None:
        """Grava um chunk recebido durante o download.

        Args:
            hash_arquivo: Hash SHA-256 do arquivo.
            index: Índice do chunk (0-based).
            data: Conteúdo binário do chunk.
        """
        caminho = self._chunk_path(hash_arquivo, index)
        caminho.parent.mkdir(parents=True, exist_ok=True)
        caminho.write_bytes(data)

    def load_chunk(self, hash_arquivo: str, index: int) -> bytes:
        """Lê um chunk, do arquivo montado (fatia) ou do diretório de download.

        Args:
            hash_arquivo: Hash SHA-256 do arquivo.
            index: Índice do chunk.

        Returns:
            Conteúdo binário do chunk.

        Raises:
            FileNotFoundError: Se o chunk não existe em nenhuma das formas.
        """
        montado = self.assembled_path(hash_arquivo)
        if montado.exists():
            with montado.open("rb") as f:
                f.seek(index * self.chunk_size)
                dados = f.read(self.chunk_size)
            if not dados:
                raise FileNotFoundError(
                    f"chunk {index} além do fim do arquivo {hash_arquivo}"
                )
            return dados

        caminho = self._chunk_path(hash_arquivo, index)
        if not caminho.exists():
            raise FileNotFoundError(f"chunk não encontrado: {caminho}")
        return caminho.read_bytes()

    def has_chunk(self, hash_arquivo: str, index: int) -> bool:
        """Indica se este peer pode fornecer o chunk index."""
        if self.assembled_path(hash_arquivo).exists():
            return 0 <= index < self.get_chunk_count(hash_arquivo)
        return self._chunk_path(hash_arquivo, index).exists()

    def get_chunk_count(self, hash_arquivo: str) -> int:
        """Número de chunks conhecidos para hash_arquivo.

        Arquivo montado: derivado do tamanho. Em download: maior índice
        presente + 1 (suficiente para responder CHUNK_LIST). Ausente: 0.
        """
        montado = self.assembled_path(hash_arquivo)
        if montado.exists():
            return chunk_count(montado.stat().st_size, self.chunk_size)

        chunks_dir = self._chunks_dir(hash_arquivo)
        if not chunks_dir.exists():
            return 0
        indices = [
            int(p.stem.removeprefix("chunk_")) for p in chunks_dir.glob("chunk_*.bin")
        ]
        return max(indices) + 1 if indices else 0

    # ------------------------------------------------------------------
    # Arquivos completos
    # ------------------------------------------------------------------

    def list_local_files(self) -> list[str]:
        """Hashes dos arquivos completos (montados) neste peer."""
        if not self.storage_dir.exists():
            return []
        return sorted(
            d.name
            for d in self.storage_dir.iterdir()
            if d.is_dir() and (d / "arquivo").exists()
        )

    def import_file(self, origem: Path) -> tuple[str, int, int]:
        """Importa um arquivo externo para o storage (comando upload).

        Args:
            origem: Caminho do arquivo no disco do usuário.

        Returns:
            Tupla (hash, tamanho_bytes, n_chunks).

        Raises:
            FileNotFoundError: Se origem não existir.
        """
        hash_arquivo = sha256_file(origem)
        tamanho = origem.stat().st_size
        destino = self.assembled_path(hash_arquivo)
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origem, destino)
        return hash_arquivo, tamanho, chunk_count(tamanho, self.chunk_size)

    def assemble_file(self, hash_arquivo: str, n_chunks: int) -> Path:
        """Concatena os chunks, valida o SHA-256 e apaga os chunks soltos.

        Monta primeiro em arquivo.tmp para que load_chunk nunca
        enxergue um arquivo montado parcial; o rename só ocorre após a
        validação do hash.

        Args:
            hash_arquivo: Hash SHA-256 esperado do conteúdo completo.
            n_chunks: Número total de chunks do arquivo.

        Returns:
            Caminho do arquivo montado.

        Raises:
            FileNotFoundError: Se algum chunk estiver faltando.
            InvalidHashError: Se o SHA-256 do montado divergir do esperado.
        """
        faltantes = [
            i for i in range(n_chunks) if not self._chunk_path(hash_arquivo, i).exists()
        ]
        if faltantes:
            raise FileNotFoundError(
                f"chunks ausentes para montar {hash_arquivo}: {faltantes[:5]}"
            )

        destino = self.assembled_path(hash_arquivo)
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporario = destino.with_name("arquivo.tmp")
        with temporario.open("wb") as f:
            for i in range(n_chunks):
                f.write(self._chunk_path(hash_arquivo, i).read_bytes())

        calculado = sha256_file(temporario)
        if calculado != hash_arquivo:
            temporario.unlink()
            raise InvalidHashError(
                f"SHA-256 do arquivo montado é {calculado}; esperado {hash_arquivo}"
            )

        temporario.replace(destino)
        shutil.rmtree(self._chunks_dir(hash_arquivo), ignore_errors=True)
        return destino

    def export_assembled(self, hash_arquivo: str, nome: str) -> Path:
        """Expõe o arquivo montado sob o nome real em downloads/<nome>.

        O <hash>/arquivo interno (endereçado por hash, usado para servir
        chunks) permanece intacto; aqui damos ao usuário o MESMO conteúdo com
        nome e extensão legíveis. Usa hardlink (sem custo de espaço) e cai para
        cópia se o SO não suportar. Um nome já ocupado por outro hash recebe um
        prefixo do hash para não colidir.

        Args:
            hash_arquivo: Hash SHA-256 do arquivo montado.
            nome: Nome legível com extensão (ex.: "Imagine.mp3").

        Returns:
            Caminho do arquivo nomeado em <storage_dir>/downloads/.

        Raises:
            FileNotFoundError: Se o arquivo montado não existir.
        """
        montado = self.assembled_path(hash_arquivo)
        if not montado.exists():
            raise FileNotFoundError(f"arquivo montado ausente para {hash_arquivo}")
        downloads = self.storage_dir / "downloads"
        downloads.mkdir(parents=True, exist_ok=True)
        nome_seguro = Path(nome).name or hash_arquivo
        destino = downloads / nome_seguro
        if destino.exists() and destino.samefile(montado):
            return destino
        if destino.exists():
            destino = downloads / f"{hash_arquivo[:8]}_{nome_seguro}"
        try:
            os.link(montado, destino)
        except OSError:
            shutil.copyfile(montado, destino)
        return destino

    def remove_file(self, hash_arquivo: str) -> None:
        """Apaga o diretório inteiro do hash (chunks e/ou arquivo montado)."""
        hash_dir = self.storage_dir / hash_arquivo
        if hash_dir.exists():
            shutil.rmtree(hash_dir)
