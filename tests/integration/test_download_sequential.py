"""Teste de integração da Fase 3: tracker + 2 peers, download sequencial.

Sobe um tracker uvicorn real e os componentes de dois peers (alice e
bob) em portas dinâmicas de 127.0.0.1 (§10). Alice importa um arquivo
de ~2 MB (8 chunks) e o registra; bob busca, baixa chunk a chunk da
alice via TCP, valida o SHA-256 e re-registra como segunda fonte.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import uvicorn

from src.common.hashing import sha256_file
from src.peer.chunk_manager import ChunkManager
from src.peer.downloader import Downloader
from src.peer.storage import Storage
from src.peer.tcp_client import PeerTCPClient
from src.peer.tcp_server import PeerTCPServer
from src.peer.tracker_client import PeerTrackerClient
from src.tracker.api import create_app
from src.tracker.index import Index
from src.tracker.persistence import init_db

#: 2 MiB = exatamente 8 chunks de 256 KiB (§7.3).
TAMANHO_ARQUIVO = 2 * 1024 * 1024
N_CHUNKS_ESPERADO = 8


def _porta_livre() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def tracker_porta(tmp_path: Path) -> Iterator[int]:
    """Tracker real servindo em porta dinâmica; derrubado ao fim do teste."""
    porta = _porta_livre()
    db = init_db(tmp_path / "tracker" / "peerspot.db")
    app = create_app(
        index=Index(),
        db=db,
        tracker_id="tracker-teste",
        trackers_conhecidos=[
            {"tracker_id": "tracker-teste", "ip": "127.0.0.1", "api_port": porta}
        ],
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
    yield porta
    server.should_exit = True
    thread.join(timeout=5)
    db.close()


@pytest.fixture()
def trackers_cfg(tracker_porta: int) -> list[dict]:
    """Lista ``trackers`` do YAML apontando para o tracker de teste."""
    return [
        {"tracker_id": "tracker-teste", "ip": "127.0.0.1", "api_port": tracker_porta}
    ]


def _subir_peer_servidor(storage: Storage) -> PeerTCPServer:
    """TCP server de chunks em porta dinâmica (porta=0 → real após bind)."""
    server = PeerTCPServer("127.0.0.1", 0, storage)
    server.start()
    assert server.started.wait(timeout=5), "TCP server do peer não subiu"
    return server


def test_upload_alice_download_bob(tmp_path: Path, trackers_cfg: list[dict]) -> None:
    """Fluxo completo: upload em alice → search/download em bob (§9 Fase 3)."""
    conteudo = os.urandom(TAMANHO_ARQUIVO)
    original = tmp_path / "musica.bin"
    original.write_bytes(conteudo)

    # --- alice: importa o arquivo e registra no tracker -----------------
    alice_storage = Storage(tmp_path / "peer-alice")
    alice_server = _subir_peer_servidor(alice_storage)
    alice_tracker = PeerTrackerClient(trackers_cfg)
    try:
        assert (
            alice_tracker.peer_hello("alice", "127.0.0.1", alice_server.porta)
            is not None
        )
        hash_arquivo, tamanho, n_chunks = alice_storage.import_file(original)
        assert tamanho == TAMANHO_ARQUIVO
        assert n_chunks == N_CHUNKS_ESPERADO
        assert (
            alice_tracker.register_file(
                "alice", hash_arquivo, nome="musica", tamanho=tamanho, n_chunks=n_chunks
            )
            is not None
        )

        # --- bob: busca, baixa sequencialmente e re-registra ------------
        bob_storage = Storage(tmp_path / "peer-bob")
        bob_tracker = PeerTrackerClient(trackers_cfg)
        bob_tcp = PeerTCPClient(timeout=10.0)
        try:
            assert bob_tracker.peer_hello("bob", "127.0.0.1", 7002) is not None

            resultado = bob_tracker.search_file("musica", str(uuid.uuid4()))
            assert resultado is not None
            assert len(resultado.resultados) == 1
            entrada = resultado.resultados[0]
            assert entrada.hash == hash_arquivo
            assert entrada.n_chunks == N_CHUNKS_ESPERADO
            assert [p.nome_peer for p in entrada.peers] == ["alice"]

            downloader = Downloader(
                nome_peer="bob",
                tracker_client=bob_tracker,
                tcp_client=bob_tcp,
                storage=bob_storage,
                chunk_manager=ChunkManager(),
            )
            caminho = downloader.download_file(hash_arquivo, "musica")

            # SHA-256 do arquivo final em bob == original (§9 Fase 3).
            assert caminho is not None
            assert sha256_file(caminho) == hash_arquivo
            assert caminho.read_bytes() == conteudo

            # Chunks individuais apagados após a montagem (sem duplicação).
            assert not (tmp_path / "peer-bob" / hash_arquivo / "chunks").exists()
            assert bob_storage.list_local_files() == [hash_arquivo]

            # bob aparece como segunda fonte do hash no tracker.
            resultado2 = bob_tracker.search_file("musica", str(uuid.uuid4()))
            assert resultado2 is not None
            fontes = {p.nome_peer for p in resultado2.resultados[0].peers}
            assert fontes == {"alice", "bob"}
        finally:
            bob_tcp.close_all()
            bob_tracker.close()
    finally:
        alice_server.stop()
        alice_tracker.close()


def test_bob_serve_chunks_apos_download(
    tmp_path: Path, trackers_cfg: list[dict]
) -> None:
    """Após baixar, bob fornece chunks lidos do arquivo montado (fatias)."""
    conteudo = os.urandom(TAMANHO_ARQUIVO)
    original = tmp_path / "musica.bin"
    original.write_bytes(conteudo)

    alice_storage = Storage(tmp_path / "peer-alice")
    alice_server = _subir_peer_servidor(alice_storage)
    alice_tracker = PeerTrackerClient(trackers_cfg)
    bob_storage = Storage(tmp_path / "peer-bob")
    bob_server = _subir_peer_servidor(bob_storage)
    bob_tracker = PeerTrackerClient(trackers_cfg)
    tcp_client = PeerTCPClient(timeout=10.0)
    try:
        alice_tracker.peer_hello("alice", "127.0.0.1", alice_server.porta)
        hash_arquivo, tamanho, n_chunks = alice_storage.import_file(original)
        alice_tracker.register_file(
            "alice", hash_arquivo, nome="musica", tamanho=tamanho, n_chunks=n_chunks
        )

        bob_tracker.peer_hello("bob", "127.0.0.1", bob_server.porta)
        downloader = Downloader(
            nome_peer="bob",
            tracker_client=bob_tracker,
            tcp_client=tcp_client,
            storage=bob_storage,
            chunk_manager=ChunkManager(),
        )
        assert downloader.download_file(hash_arquivo, "musica") is not None

        # bob agora serve CHUNK_LIST e chunks íntegros a partir do montado.
        indices = tcp_client.request_chunk_list(
            "127.0.0.1", bob_server.porta, hash_arquivo
        )
        assert indices == list(range(N_CHUNKS_ESPERADO))
        chunk0 = tcp_client.download_chunk(
            "127.0.0.1", bob_server.porta, hash_arquivo, 0
        )
        assert chunk0 == conteudo[: 256 * 1024]
        ultimo = tcp_client.download_chunk(
            "127.0.0.1", bob_server.porta, hash_arquivo, N_CHUNKS_ESPERADO - 1
        )
        assert ultimo == conteudo[(N_CHUNKS_ESPERADO - 1) * 256 * 1024 :]
    finally:
        tcp_client.close_all()
        alice_server.stop()
        bob_server.stop()
        alice_tracker.close()
        bob_tracker.close()
