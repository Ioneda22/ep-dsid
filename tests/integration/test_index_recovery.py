"""Recuperação do índice quando um tracker volta SEM estado (Bug 1).

Cenário reportado: upload de uma música → todos os trackers caem → um volta
com o índice vazio → uma busca por nome não encontra nada, embora o arquivo
ainda esteja no peer (ls). Como o índice é em memória e o SEED_REPORT
periódico só carrega hashes (não reconstrói nome→hash), a busca falharia.

Correção (sem tocar no protocolo): quando a busca volta vazia e há arquivos
locais, a CLI re-anuncia o peer e re-registra seus arquivos via PEER_HELLO +
REGISTER_FILE (mensagens já existentes, que carregam os metadados) e refaz a
busca uma vez.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn

from src.peer.chunk_manager import ChunkManager
from src.peer.cli import PeerCLI
from src.peer.downloader import Downloader
from src.peer.name_registry import NameRegistry
from src.peer.playlist_store import PlaylistStore
from src.peer.storage import Storage
from src.peer.tcp_client import PeerTCPClient
from src.peer.tracker_client import PeerTrackerClient
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db


def _porta_livre() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def tracker(tmp_path: Path) -> Iterator[tuple[Index, int]]:
    """Tracker real (uvicorn em thread); devolve (índice, porta)."""
    idx = Index(tracker_id="tracker-1")
    porta = _porta_livre()
    db = init_db(tmp_path / "tracker.db")
    app = create_app(index=idx, db=db, tracker_id="tracker-1", trackers_conhecidos=[])
    config = uvicorn.Config(
        app, host="127.0.0.1", port=porta, log_config=None, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    prazo = time.time() + 10
    while not server.started:
        if time.time() > prazo:
            pytest.fail("tracker não subiu em 10s")
        time.sleep(0.01)
    yield idx, porta
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


def _peer_cli(tmp_path: Path, porta: int) -> tuple[PeerCLI, Storage, PeerTrackerClient]:
    storage = Storage(tmp_path / "peer")
    registry = NameRegistry(tmp_path / "peer")
    client = PeerTrackerClient(
        [{"tracker_id": "tracker-1", "ip": "127.0.0.1", "api_port": porta}]
    )
    downloader = Downloader(
        nome_peer="alice",
        tracker_client=client,
        tcp_client=PeerTCPClient(),
        storage=storage,
        chunk_manager=ChunkManager(),
    )
    cli = PeerCLI(
        "alice", storage, client, downloader, registry, PlaylistStore(tmp_path / "peer")
    )
    return cli, storage, client


def test_busca_recupera_indice_apos_tracker_voltar_vazio(
    tmp_path: Path, tracker: tuple[Index, int]
) -> None:
    idx, porta = tracker
    cli, storage, client = _peer_cli(tmp_path, porta)

    # Upload de "musica" (popula storage, NameRegistry e o índice do tracker).
    origem = tmp_path / "musica.bin"
    origem.write_bytes(b"x" * 1000)
    client.peer_hello("alice", "127.0.0.1", 7001)
    cli._cmd_upload(str(origem))
    hash_arquivo = next(iter(storage.list_local_files()))
    assert [e.hash for e in idx.search_by_name("musica")] == [hash_arquivo]

    # Simula "todos os trackers caíram e um voltou vazio": zera o índice
    # (inclusive a presença — o tracker esqueceu até o PEER_HELLO).
    with idx._lock:
        idx.nome_to_hashes.clear()
        idx.hash_to_metadata.clear()
        idx.hash_to_peers.clear()
        idx.nome_peer_to_endereco.clear()
        idx.tombstones.clear()

    # ls ainda mostra o arquivo local, mas o tracker não sabe de nada.
    assert storage.list_local_files() == [hash_arquivo]
    assert idx.search_by_name("musica") == []

    # A busca da CLI dispara a recuperação (re-hello + re-registro) e encontra.
    cli._cmd_search("musica")
    assert [e.hash for e in cli._ultima_busca] == [hash_arquivo]
    assert [e.hash for e in idx.search_by_name("musica")] == [hash_arquivo]
    client.close()


def test_busca_vazia_sem_arquivos_locais_nao_reregistra(
    tmp_path: Path, tracker: tuple[Index, int]
) -> None:
    # Sem arquivos locais, uma busca vazia legítima não tenta recuperar nada.
    _, porta = tracker
    cli, _storage, client = _peer_cli(tmp_path, porta)
    client.peer_hello("alice", "127.0.0.1", 7001)
    cli._cmd_search("inexistente")
    assert cli._ultima_busca == []
    client.close()
