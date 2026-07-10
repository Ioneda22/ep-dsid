"""Persistência durável do tracker em SQLite.

Guarda apenas os usuários (presença histórica dos peers). Playlists NÃO ficam
aqui: são estado LOCAL do peer (src.peer.playlist_store.PlaylistStore),
disponível mesmo sem nenhum tracker no ar. O índice de arquivos nunca é
persistido — vive em memória no src.tracker.index.Index.

O sqlite3 da stdlib não garante serialização entre threads em todas
as builds, e o uvicorn despacha rotas síncronas num threadpool; por isso
a conexão é encapsulada em TrackerDB com um threading.Lock.
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
"""


class TrackerDB:
    """Conexão SQLite do tracker, serializada por lock.

    Use init_db para construir uma instância já com o schema
    aplicado. O relógio é injetável para testes determinísticos.
    """

    def __init__(
        self, conn: sqlite3.Connection, clock: Callable[[], float] = time.time
    ) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self._clock = clock

    def registrar_usuario(self, nome_peer: str) -> None:
        """Insere o usuário se ainda não existir (idempotente).

        Chamado a cada PEER_HELLO — repetir o hello não duplica linha.
        """
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO usuarios (nome_peer, criado_em) VALUES (?, ?)",
                (nome_peer, self._clock()),
            )
            self._conn.commit()

    def listar_usuarios(self) -> list[str]:
        """Devolve os nome_peer conhecidos, em ordem alfabética."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT nome_peer FROM usuarios ORDER BY nome_peer"
            ).fetchall()
        return [nome for (nome,) in rows]

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        with self._lock:
            self._conn.close()


def init_db(db_path: Path, clock: Callable[[], float] = time.time) -> TrackerDB:
    """Abre (criando se preciso) o banco SQLite e aplica o schema.

    Args:
        db_path: Caminho do arquivo .db; diretórios pais são criados.
        clock: Fonte de tempo injetável para testes.

    Returns:
        Um TrackerDB pronto para uso por múltiplas threads.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: o lock interno do TrackerDB serializa o acesso.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.executescript(_SCHEMA)
    conn.commit()
    logger.info("SQLite inicializado em %s", db_path)
    return TrackerDB(conn, clock)
