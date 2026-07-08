"""Helpers para subir um cluster de trackers reais em threads (§10).

Cada nó do cluster tem seu próprio ``Index``, servidor de sincronização TCP
(``SyncServer``) e API REST (uvicorn), tudo em ``127.0.0.1`` com portas
dinâmicas. ``com_flooding=False`` desliga o ``SyncClient`` (nenhuma
propagação de SYNC_TABLE), útil para exercitar o SEARCH_FORWARD isolado.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import uvicorn

from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import TrackerDB, init_db
from src.tracker.rebalance import RebalanceManager
from src.tracker.routing import SearchRouter
from src.tracker.sync_client import KnownTracker, SyncClient
from src.tracker.sync_server import SyncServer


def porta_livre() -> int:
    """Pede ao SO uma porta TCP livre em 127.0.0.1."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def aguardar(
    condicao: Callable[[], bool], timeout: float = 3.0, intervalo: float = 0.05
) -> bool:
    """Espera ``condicao()`` virar True dentro de ``timeout`` segundos."""
    prazo = time.monotonic() + timeout
    while time.monotonic() < prazo:
        if condicao():
            return True
        time.sleep(intervalo)
    return condicao()


@dataclass
class TrackerNode:
    """Um tracker completo rodando em threads do processo de teste."""

    tracker_id: str
    index: Index
    sync_server: SyncServer
    sync_client: SyncClient | None
    search_router: SearchRouter
    api_url: str
    api_port: int
    rebalance: RebalanceManager | None
    _uvicorn: uvicorn.Server
    _thread: threading.Thread
    _db: TrackerDB

    def parar(self) -> None:
        """Derruba API, sync server e fecha o banco."""
        self._uvicorn.should_exit = True
        self._thread.join(timeout=5)
        self.sync_server.stop()
        self._db.close()


def _subir_api(
    node_app: object, api_port: int
) -> tuple[uvicorn.Server, threading.Thread]:
    config = uvicorn.Config(
        node_app, host="127.0.0.1", port=api_port, log_config=None, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server, thread


@contextmanager
def cluster_de_trackers(
    tmp_path: Path,
    ids: tuple[str, ...] = ("tracker-1", "tracker-2", "tracker-3"),
    com_flooding: bool = True,
    search_timeout: float = 2.0,
    clock: Callable[[], float] = time.time,
) -> Iterator[dict[str, TrackerNode]]:
    """Sobe ``len(ids)`` trackers conectados entre si e derruba ao sair.

    Os sync servers sobem primeiro (porta 0 → dinâmica); só então cada nó
    monta sua lista de ``KnownTracker`` com as portas efetivas dos demais.

    ``clock`` é injetado em TODOS os índices (§10): passe um relógio mutável para
    exercitar de forma determinística o que depende de tempo (expiração,
    failure detection) sem dormir.
    """
    indices = {tid: Index(clock=clock, tracker_id=tid) for tid in ids}
    sync_servers: dict[str, SyncServer] = {}
    for tid in ids:
        servidor = SyncServer(tid, "127.0.0.1", 0, indices[tid])
        servidor.start()
        assert servidor.started.wait(timeout=5), f"sync server de {tid} não subiu"
        sync_servers[tid] = servidor

    # Portas de API escolhidas antes do loop: assim o mapa tracker_id -> (ip,
    # api_port) do rebalance já conhece o endereço REST de todos os trackers.
    api_ports = {tid: porta_livre() for tid in ids}
    api_map = {tid: ("127.0.0.1", api_ports[tid]) for tid in ids}

    nodes: dict[str, TrackerNode] = {}
    try:
        for tid in ids:
            conhecidos = [
                KnownTracker(outro, "127.0.0.1", sync_servers[outro].porta)
                for outro in ids
                if outro != tid
            ]
            sync_client = (
                SyncClient(tid, conhecidos, index=indices[tid], timeout_seconds=1.0)
                if com_flooding
                else None
            )
            # Habilita o servidor a disparar SYNC_PULL na detecção de lacuna e a
            # responder TRACKER_REJOIN (o cluster resolve portas antes dos clients).
            sync_servers[tid].sync_client = sync_client
            rebalance = (
                RebalanceManager(tid, indices[tid], sync_client, api_map)
                if sync_client is not None
                else None
            )
            sync_servers[tid].rebalance = rebalance
            router = SearchRouter(
                tid, conhecidos, indices[tid], timeout_seconds=search_timeout
            )
            db = init_db(tmp_path / f"{tid}.db")
            api_port = api_ports[tid]
            app = create_app(
                index=indices[tid],
                db=db,
                tracker_id=tid,
                trackers_conhecidos=[],
                sync_client=sync_client,
                search_router=router,
            )
            server, thread = _subir_api(app, api_port)
            nodes[tid] = TrackerNode(
                tracker_id=tid,
                index=indices[tid],
                sync_server=sync_servers[tid],
                sync_client=sync_client,
                search_router=router,
                api_url=f"http://127.0.0.1:{api_port}",
                api_port=api_port,
                rebalance=rebalance,
                _uvicorn=server,
                _thread=thread,
                _db=db,
            )
        for node in nodes.values():
            assert aguardar(
                lambda servidor=node._uvicorn: servidor.started, timeout=10
            ), f"API de {node.tracker_id} não subiu"
        yield nodes
    finally:
        for node in nodes.values():
            node.parar()
        for tid, servidor in sync_servers.items():
            if tid not in nodes:
                servidor.stop()
