"""Playlists locais do peer, persistidas em JSON no storage do peer.

As playlists são estado LOCAL do peer (como a lista de arquivos completos e o
NameRegistry), não estado do tracker. Ficam em <storage_dir>/playlists.json e
por isso continuam disponíveis mesmo sem nenhum tracker no ar — o comando
`playlist list` funciona offline, igual ao `ls` de músicas.

Cada playlist tem um id inteiro monotônico (não reutilizado após deleção), um
nome e uma lista ordenada de hashes. O dono é implícito: é este peer.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class PlaylistStore:
    """CRUD de playlists locais, persistido em <storage_dir>/playlists.json."""

    def __init__(self, storage_dir: str | Path) -> None:
        """Ancora o store no storage do peer e carrega o que houver em disco."""
        self._caminho = Path(storage_dir) / "playlists.json"
        self._proximo_id = 1
        self._playlists: dict[int, dict[str, object]] = {}
        self._carregar()

    def _carregar(self) -> None:
        """Lê o JSON existente; começa vazio se ausente ou ilegível."""
        if not self._caminho.exists():
            return
        try:
            dados = json.loads(self._caminho.read_text(encoding="utf-8"))
            self._proximo_id = int(dados["proximo_id"])
            self._playlists = {
                int(pid): {"nome": str(pl["nome"]), "itens": list(pl["itens"])}
                for pid, pl in dados["playlists"].items()
            }
        except (json.JSONDecodeError, OSError, KeyError, ValueError, TypeError):
            logger.exception(
                "playlists.json ilegível em %s; começando vazio", self._caminho
            )
            self._proximo_id = 1
            self._playlists = {}

    def _salvar(self) -> None:
        """Grava o store inteiro (arquivo pequeno; conveniência, não hot path)."""
        self._caminho.parent.mkdir(parents=True, exist_ok=True)
        dados = {
            "proximo_id": self._proximo_id,
            "playlists": {str(pid): pl for pid, pl in self._playlists.items()},
        }
        self._caminho.write_text(
            json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def criar(self, nome: str) -> int:
        """Cria uma playlist vazia e devolve seu id (não reutilizado)."""
        pid = self._proximo_id
        self._proximo_id += 1
        self._playlists[pid] = {"nome": nome, "itens": []}
        self._salvar()
        return pid

    def listar(self) -> list[dict[str, object]]:
        """Lista as playlists (id e nome), em ordem de criação (id)."""
        return [
            {"id": pid, "nome": pl["nome"]}
            for pid, pl in sorted(self._playlists.items())
        ]

    def obter(self, playlist_id: int) -> dict[str, object] | None:
        """Devolve {nome, itens: [hash, ...]} ou None se não existir."""
        pl = self._playlists.get(playlist_id)
        if pl is None:
            return None
        return {"nome": pl["nome"], "itens": list(pl["itens"])}  # type: ignore[arg-type]

    def adicionar_item(self, playlist_id: int, hash_arquivo: str) -> bool:
        """Adiciona um hash ao fim da playlist (sem duplicar).

        Returns:
            True se adicionado (ou já presente); False se a playlist não existe.
        """
        pl = self._playlists.get(playlist_id)
        if pl is None:
            return False
        itens: list[str] = pl["itens"]  # type: ignore[assignment]
        if hash_arquivo not in itens:
            itens.append(hash_arquivo)
            self._salvar()
        return True

    def remover_item(self, playlist_id: int, hash_arquivo: str) -> bool:
        """Remove um hash da playlist.

        Returns:
            True se a playlist existe (removido ou não estava lá); False se a
            playlist não existe.
        """
        pl = self._playlists.get(playlist_id)
        if pl is None:
            return False
        itens: list[str] = pl["itens"]  # type: ignore[assignment]
        if hash_arquivo in itens:
            itens.remove(hash_arquivo)
            self._salvar()
        return True

    def deletar(self, playlist_id: int) -> bool:
        """Apaga a playlist. Returns True se existia, False caso contrário."""
        if self._playlists.pop(playlist_id, None) is None:
            return False
        self._salvar()
        return True
