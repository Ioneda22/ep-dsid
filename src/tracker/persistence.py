"""Persistência durável do tracker em SQLite (§6.1, camada 3).

Guarda apenas os dados duráveis: usuários e playlists. O índice de
arquivos **nunca** é persistido (§11.4) — vive em memória no
:class:`src.tracker.index.Index`.

O ``sqlite3`` da stdlib não garante serialização entre threads em todas
as builds, e o uvicorn despacha rotas síncronas num threadpool; por isso
a conexão é encapsulada em :class:`TrackerDB` com um ``threading.Lock``.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usuarios (
    nome_peer TEXT PRIMARY KEY,
    criado_em REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS playlists (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    dono      TEXT NOT NULL REFERENCES usuarios(nome_peer),
    nome      TEXT NOT NULL,
    criada_em REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS playlist_itens (
    playlist_id INTEGER NOT NULL REFERENCES playlists(id),
    hash        TEXT NOT NULL,
    ordem       INTEGER NOT NULL,
    PRIMARY KEY (playlist_id, ordem)
);
"""


class TrackerDB:
    """Conexão SQLite do tracker, serializada por lock.

    Use :func:`init_db` para construir uma instância já com o schema
    aplicado. O relógio é injetável para testes determinísticos (§10).
    """

    def __init__(
        self, conn: sqlite3.Connection, clock: Callable[[], float] = time.time
    ) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self._clock = clock

    def registrar_usuario(self, nome_peer: str) -> None:
        """Insere o usuário se ainda não existir (idempotente).

        Chamado a cada ``PEER_HELLO`` — repetir o hello não duplica linha.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO usuarios (nome_peer, criado_em) VALUES (?, ?)",
                (nome_peer, self._clock()),
            )
            self._conn.commit()

    def listar_usuarios(self) -> list[str]:
        """Devolve os ``nome_peer`` conhecidos, em ordem alfabética."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT nome_peer FROM usuarios ORDER BY nome_peer"
            ).fetchall()
        return [nome for (nome,) in rows]

    # ------------------------------------------------------------------
    # Playlists — stubs até a Fase 6 (§9)
    # ------------------------------------------------------------------

    def criar_playlist(self, dono: str, nome: str) -> int:
        """Stub: CRUD de playlists chega na Fase 6."""
        raise NotImplementedError("playlists serão implementadas na Fase 6")

    def adicionar_item_playlist(self, playlist_id: int, hash_arquivo: str) -> None:
        """Stub: CRUD de playlists chega na Fase 6."""
        raise NotImplementedError("playlists serão implementadas na Fase 6")

    def listar_playlist(self, playlist_id: int) -> list[str]:
        """Stub: CRUD de playlists chega na Fase 6."""
        raise NotImplementedError("playlists serão implementadas na Fase 6")

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        with self._lock:
            self._conn.close()


def init_db(db_path: Path, clock: Callable[[], float] = time.time) -> TrackerDB:
    """Abre (criando se preciso) o banco SQLite e aplica o schema.

    Args:
        db_path: Caminho do arquivo ``.db``; diretórios pais são criados.
        clock: Fonte de tempo injetável para testes.

    Returns:
        Um :class:`TrackerDB` pronto para uso por múltiplas threads.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: o lock interno do TrackerDB serializa o acesso.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.executescript(_SCHEMA)
    conn.commit()
    logger.info("SQLite inicializado em %s", db_path)
    return TrackerDB(conn, clock)
