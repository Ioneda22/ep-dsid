"""Integração de download paralelo entre fontes complementares.

1 tracker + 3 peers. Alice tem os chunks 0-3 e Bob os chunks 2-5 de um arquivo de
6 chunks. Carol precisa das duas fontes (0/1 só na Alice, 4/5 só no Bob) e o
download distribui os pedidos entre elas em paralelo. Valida o SHA-256 final e
que AMBAS as fontes foram contatadas.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn

from src.peer.chunk_manager import ChunkManager
from src.peer.downloader import Downloader
from src.peer.storage import Storage
from src.peer.tcp_client import PeerTCPClient
from src.peer.tcp_server import PeerTCPServer
from src.peer.tracker_client import PeerTrackerClient
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db
from tests.integration.cluster import porta_livre

N_CHUNKS = 6
CHUNK_BYTES = 4096


@pytest.fixture()
def tracker_cfg(tmp_path: Path) -> Iterator[list[dict]]:
    porta = porta_livre()
    db = init_db(tmp_path / "tracker" / "peerspot.db")
    app = create_app(
        index=Index(tracker_id="tracker-1"),
        db=db,
        tracker_id="tracker-1",
        trackers_conhecidos=[],
    )
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
    yield [{"tracker_id": "tracker-1", "ip": "127.0.0.1", "api_port": porta}]
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


class _TCPClientEspiao(PeerTCPClient):
    """Registra os endereços de fonte de cada CHUNK_REQUEST atendido."""

    def __init__(self, timeout: float = 10.0) -> None:
        super().__init__(timeout)
        self.contatadas: set[tuple[str, int]] = set()
        self._lock = threading.Lock()

    def download_chunk(
        self, ip: str, porta: int, hash_arquivo: str, chunk_index: int
    ) -> bytes | None:
        dados = super().download_chunk(ip, porta, hash_arquivo, chunk_index)
        if dados is not None:
            with self._lock:
                self.contatadas.add((ip, porta))
        return dados


def _subir_servidor(storage: Storage) -> PeerTCPServer:
    server = PeerTCPServer("127.0.0.1", 0, storage)
    server.start()
    assert server.started.wait(timeout=5)
    return server


def test_carol_baixa_de_alice_e_bob_em_paralelo(
    tmp_path: Path, tracker_cfg: list[dict]
) -> None:
    # Arquivo de 6 chunks; hash = SHA-256 da concatenação.
    chunks = [bytes([i]) * CHUNK_BYTES for i in range(N_CHUNKS)]
    conteudo = b"".join(chunks)
    hash_arquivo = hashlib.sha256(conteudo).hexdigest()

    alice_storage = Storage(tmp_path / "alice")
    bob_storage = Storage(tmp_path / "bob")
    for i in range(0, 4):  # alice: chunks 0,1,2,3
        alice_storage.save_chunk(hash_arquivo, i, chunks[i])
    for i in range(2, 6):  # bob: chunks 2,3,4,5
        bob_storage.save_chunk(hash_arquivo, i, chunks[i])

    alice_server = _subir_servidor(alice_storage)
    bob_server = _subir_servidor(bob_storage)
    alice_tracker = PeerTrackerClient(tracker_cfg)
    bob_tracker = PeerTrackerClient(tracker_cfg)

    carol_storage = Storage(tmp_path / "carol")
    carol_tracker = PeerTrackerClient(tracker_cfg)
    carol_tcp = _TCPClientEspiao(timeout=5.0)
    try:
        alice_tracker.peer_hello("alice", "127.0.0.1", alice_server.porta)
        bob_tracker.peer_hello("bob", "127.0.0.1", bob_server.porta)
        # alice registra os metadados (n_chunks=6); bob entra como 2ª fonte.
        alice_tracker.register_file(
            "alice",
            hash_arquivo,
            nome="musica",
            tamanho=len(conteudo),
            n_chunks=N_CHUNKS,
        )
        bob_tracker.register_file("bob", hash_arquivo)

        carol_tracker.peer_hello("carol", "127.0.0.1", 7003)
        downloader = Downloader(
            nome_peer="carol",
            tracker_client=carol_tracker,
            tcp_client=carol_tcp,
            storage=carol_storage,
            chunk_manager=ChunkManager(),
            download_pool_size=8,
        )
        caminho = downloader.download_file(hash_arquivo, "musica")

        assert caminho is not None
        assert hashlib.sha256(caminho.read_bytes()).hexdigest() == hash_arquivo
        assert caminho.read_bytes() == conteudo
        # Ambas as fontes foram necessárias e contatadas (0/1 só alice, 4/5 só bob).
        contatadas = carol_tcp.contatadas
        assert ("127.0.0.1", alice_server.porta) in contatadas
        assert ("127.0.0.1", bob_server.porta) in contatadas
        # carol virou 3ª fonte no tracker.
        resultado = carol_tracker.search_file("musica", "q-final")
        assert resultado is not None
        fontes = {p.nome_peer for p in resultado.resultados[0].peers}
        assert fontes == {"alice", "bob", "carol"}
    finally:
        carol_tcp.close_all()
        alice_server.stop()
        bob_server.stop()
        alice_tracker.close()
        bob_tracker.close()
        carol_tracker.close()
