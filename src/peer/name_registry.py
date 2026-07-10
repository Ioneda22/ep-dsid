"""Registro local nome legível ↔ hash, persistido em JSON no storage do peer.

Permite que os comandos da CLI (list, status, remove, download) operem por
nome de música em vez do hash SHA-256 de 64 caracteres. É estado local de
conveniência do peer — NÃO faz parte do índice distribuído nem do protocolo
(o tracker continua sendo a autoridade sobre nome→hash na busca). Sobrevive
a reinícios do peer porque o storage_dir persiste em disco.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class NameRegistry:
    """Mapa hash→nome persistido em <storage_dir>/nomes.json."""

    def __init__(self, storage_dir: str | Path) -> None:
        """Ancora o registro no storage do peer e carrega o que houver em disco."""
        self._caminho = Path(storage_dir) / "nomes.json"
        self._hash_para_nome: dict[str, str] = {}
        self._carregar()

    def _carregar(self) -> None:
        """Lê o JSON existente; começa vazio se ausente ou ilegível."""
        if not self._caminho.exists():
            return
        try:
            dados = json.loads(self._caminho.read_text(encoding="utf-8"))
            self._hash_para_nome = {str(k): str(v) for k, v in dados.items()}
        except (json.JSONDecodeError, OSError, AttributeError):
            logger.exception(
                "nomes.json ilegível em %s; começando vazio", self._caminho
            )
            self._hash_para_nome = {}

    def _salvar(self) -> None:
        """Grava o mapa inteiro (arquivo pequeno; conveniência, não hot path)."""
        self._caminho.parent.mkdir(parents=True, exist_ok=True)
        self._caminho.write_text(
            json.dumps(self._hash_para_nome, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def registrar(self, hash_arquivo: str, nome: str) -> None:
        """Associa um nome legível a um hash (upload ou download concluído)."""
        self._hash_para_nome[hash_arquivo] = nome
        self._salvar()

    def esquecer(self, hash_arquivo: str) -> None:
        """Remove a associação de um hash (após remove local)."""
        if self._hash_para_nome.pop(hash_arquivo, None) is not None:
            self._salvar()

    def nome(self, hash_arquivo: str) -> str | None:
        """Nome legível conhecido para o hash, ou None."""
        return self._hash_para_nome.get(hash_arquivo)

    def hashes_por_nome(self, nome: str) -> list[str]:
        """Hashes cujo nome casa (case-insensitive) com o termo dado."""
        alvo = nome.casefold()
        return [h for h, n in self._hash_para_nome.items() if n.casefold() == alvo]
