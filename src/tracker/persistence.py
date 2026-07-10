"""Persistência durável do tracker em SQLite.

Guarda apenas os dados duráveis: usuários e playlists. O índice de
arquivos nunca é persistido — vive em memória no
src.tracker.index.Index.

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
from typing import Any

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

    # ------------------------------------------------------------------
    # Playlists — dados de usuário, locais ao tracker.
    #
    # Não são propagadas entre trackers via SYNC_TABLE: só o índice de
    # arquivos é replicado. Uma playlist só existe no tracker onde foi
    # criada (limitação aceita e conhecida).
    # ------------------------------------------------------------------

    def criar_playlist(self, dono: str, nome: str) -> int:
        """Cria uma playlist e devolve seu id autoincrementado."""
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO playlists (dono, nome, criada_em) VALUES (?, ?, ?)",
                (dono, nome, self._clock()),
            )
            self._conn.commit()
            return int(cur.lastrowid)  # type: ignore[arg-type]

    def listar_playlists(self, dono: str) -> list[dict[str, Any]]:
        """Lista as playlists de um dono, em ordem de criação (id)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, nome FROM playlists WHERE dono = ? ORDER BY id",
                (dono,),
            ).fetchall()
        return [{"id": pid, "nome": nome, "dono": dono} for (pid, nome) in rows]

    def proxima_ordem(self, playlist_id: int) -> int:
        """Devolve a próxima ordem livre (MAX+1), robusta a remoções."""
        with self._lock:
            (maxo,) = self._conn.execute(
                "SELECT COALESCE(MAX(ordem), -1) FROM playlist_itens "
                "WHERE playlist_id = ?",
                (playlist_id,),
            ).fetchone()
        return int(maxo) + 1

    def adicionar_item(self, playlist_id: int, hash_arquivo: str, ordem: int) -> None:
        """Insere um hash na playlist na posição ordem (única por playlist)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO playlist_itens (playlist_id, hash, ordem) "
                "VALUES (?, ?, ?)",
                (playlist_id, hash_arquivo, ordem),
            )
            self._conn.commit()

    def remover_item(self, playlist_id: int, hash_arquivo: str) -> None:
        """Remove um hash da playlist (no-op se o hash não estiver nela)."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM playlist_itens WHERE playlist_id = ? AND hash = ?",
                (playlist_id, hash_arquivo),
            )
            self._conn.commit()

    def obter_playlist(self, playlist_id: int) -> dict[str, Any] | None:
        """Devolve {nome, dono, itens: [hash, ...]} ou None se não existir."""
        with self._lock:
            cabecalho = self._conn.execute(
                "SELECT dono, nome FROM playlists WHERE id = ?", (playlist_id,)
            ).fetchone()
            if cabecalho is None:
                return None
            itens = self._conn.execute(
                "SELECT hash FROM playlist_itens WHERE playlist_id = ? ORDER BY ordem",
                (playlist_id,),
            ).fetchall()
        dono, nome = cabecalho
        return {"nome": nome, "dono": dono, "itens": [h for (h,) in itens]}

    def deletar_playlist(self, playlist_id: int) -> None:
        """Apaga a playlist e todos os seus itens (no-op se não existir)."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM playlist_itens WHERE playlist_id = ?", (playlist_id,)
            )
            self._conn.execute("DELETE FROM playlists WHERE id = ?", (playlist_id,))
            self._conn.commit()

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
