"""Integração: reconciliação por ``seq`` + ``SYNC_PULL`` + ``SYNC_DIGEST``.

Sobe 3 trackers reais (API uvicorn + sync server TCP) em portas dinâmicas.
Simula um delta perdido escrevendo DIRETO no índice do tracker-1 (sem passar
pela API, logo sem flooding), abrindo uma lacuna de ``seq`` nas réplicas. Valida:

* detecção inline: a próxima ``SYNC_TABLE`` daquela origem revela a lacuna, que
  dispara um ``SYNC_PULL(desde_seq capturado)`` e o índice converge;
* backstop por digest: quando a ÚLTIMA escrita se perde e a origem silencia, um
  ``SYNC_DIGEST`` detecta e repara;
* reintegração: um tracker que volta reconstrói o índice via
  ``TRACKER_REJOIN`` -> ``TRACKER_LIST`` -> ``SYNC_PULL(desde_seq=0)``,
  inicializando ``visto`` e ``meu_seq`` (main.tex §11.3 e §12.3).

Nenhum cenário reenvia o índice inteiro — só o que de fato divergiu.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from src.tracker.index import Index
from src.tracker.sync_client import KnownTracker, SyncClient
from tests.integration.cluster import TrackerNode, aguardar, cluster_de_trackers

HASH1 = "11" * 32
HASH2 = "22" * 32
HASH3 = "33" * 32


@pytest.fixture()
def trackers(tmp_path: Path) -> Iterator[dict[str, TrackerNode]]:
    with cluster_de_trackers(tmp_path, com_flooding=True) as nodes:
        yield nodes


def _hello(api_url: str, nome_peer: str, porta: int) -> None:
    resposta = httpx.post(
        f"{api_url}/peers/hello",
        json={
            "type": "PEER_HELLO",
            "nome_peer": nome_peer,
            "ip": "127.0.0.1",
            "porta": porta,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200


def _register(api_url: str, nome_peer: str, hash_arquivo: str, nome: str) -> None:
    resposta = httpx.post(
        f"{api_url}/files/register",
        json={
            "type": "REGISTER_FILE",
            "nome_peer": nome_peer,
            "hash": hash_arquivo,
            "nome": nome,
            "tamanho": 100,
            "n_chunks": 1,
        },
        timeout=5.0,
    )
    assert resposta.status_code == 200


def _hashes_locais(index: Index) -> set[str]:
    return {
        h
        for nome in ("Um", "Dois", "Tres")
        for h in (e.hash for e in index.search_by_name(nome))
    }


# ---------------------------------------------------------------------------
# Detecção inline: lacuna de seq -> SYNC_PULL(desde capturado)
# ---------------------------------------------------------------------------


def test_lacuna_inline_dispara_pull_e_converge(
    trackers: dict[str, TrackerNode],
) -> None:
    t1 = trackers["tracker-1"]
    _hello(t1.api_url, "alice", 7001)
    _register(t1.api_url, "alice", HASH1, "Um")  # seq 1, flooded
    assert aguardar(
        lambda: all(
            _hashes_locais(trackers[t].index) == {HASH1}
            for t in ("tracker-2", "tracker-3")
        )
    ), "seq 1 não convergiu"

    # Delta perdido: escrita DIRETA no índice do tracker-1 (seq 2, sem flooding).
    t1.index.register_file("alice", HASH2, nome="Dois", tamanho=100, n_chunks=1)
    # Próxima escrita flooded (seq 3) revela a lacuna (3 > visto+1) nas réplicas.
    _register(t1.api_url, "alice", HASH3, "Tres")  # seq 3, flooded

    assert aguardar(
        lambda: all(
            _hashes_locais(trackers[t].index) == {HASH1, HASH2, HASH3}
            for t in ("tracker-2", "tracker-3")
        ),
        timeout=3.0,
    ), "SYNC_PULL não reparou a lacuna de seq"
    # O reparo não reabre pendências depois de aplicado.
    assert aguardar(
        lambda: all(
            trackers[t].index.pendencias() == {} for t in ("tracker-2", "tracker-3")
        )
    ), "pendência não foi fechada após o pull"


# ---------------------------------------------------------------------------
# Backstop por digest: última escrita perdida + origem em silêncio
# ---------------------------------------------------------------------------


def test_digest_repara_ultima_escrita_perdida(trackers: dict[str, TrackerNode]) -> None:
    t1 = trackers["tracker-1"]
    _hello(t1.api_url, "alice", 7001)
    _register(t1.api_url, "alice", HASH1, "Um")  # seq 1, flooded
    assert aguardar(
        lambda: _hashes_locais(trackers["tracker-2"].index) == {HASH1}
    ), "seq 1 não convergiu"

    # ÚLTIMA escrita perdida: direta no índice (seq 2, sem flooding) e sem mais
    # escritas — a detecção inline não tem seq posterior para revelar a lacuna.
    t1.index.register_file("alice", HASH2, nome="Dois", tamanho=100, n_chunks=1)
    assert _hashes_locais(trackers["tracker-2"].index) == {HASH1}  # ainda não sabe

    # O digest periódico (aqui disparado à mão) expõe visto[t1]=2 > local=1.
    assert t1.sync_client is not None
    t1.sync_client.propagar_digest(t1.index.versoes())

    assert aguardar(
        lambda: _hashes_locais(trackers["tracker-2"].index) == {HASH1, HASH2},
        timeout=3.0,
    ), "SYNC_DIGEST não repôs a última escrita perdida"


# ---------------------------------------------------------------------------
# Reintegração: TRACKER_REJOIN -> TRACKER_LIST -> SYNC_PULL(desde_seq=0)
# ---------------------------------------------------------------------------


def test_reintegracao_reconstroi_indice_e_inicializa_seq(
    trackers: dict[str, TrackerNode],
) -> None:
    t1 = trackers["tracker-1"]
    _hello(t1.api_url, "alice", 7001)
    _register(t1.api_url, "alice", HASH1, "Um")  # seq 1
    _register(t1.api_url, "alice", HASH2, "Dois")  # seq 2
    assert aguardar(
        lambda: _hashes_locais(trackers["tracker-2"].index) == {HASH1, HASH2}
    ), "cluster não convergiu antes da reintegração"

    # Simula o tracker-1 voltando com índice vazio (não persistido): novo Index
    # + SyncClient com a membership do YAML apontando para 2 e 3.
    novo_index = Index(tracker_id="tracker-1")
    conhecidos = [
        KnownTracker("tracker-2", "127.0.0.1", trackers["tracker-2"].sync_server.porta),
        KnownTracker("tracker-3", "127.0.0.1", trackers["tracker-3"].sync_server.porta),
    ]
    rejoiner = SyncClient(
        "tracker-1", conhecidos, index=novo_index, timeout_seconds=2.0
    )

    aplicadas = rejoiner.reintegrar("127.0.0.1", 9999)

    assert aplicadas >= 2
    # Índice reconstruído (fontes de tracker-1 recuperadas de uma réplica viva).
    assert _hashes_locais(novo_index) == {HASH1, HASH2}
    # meu_seq restaurado = maior seq das entradas com origem == tracker-1 (2),
    # evitando reuso de seq após reinício sem persistir em disco (main.tex §12.3).
    assert novo_index.versoes()["tracker-1"] == 2
