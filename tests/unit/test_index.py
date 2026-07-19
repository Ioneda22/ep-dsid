"""Testes unitários do índice em memória do tracker."""

from __future__ import annotations

import pytest

from src.common.errors import NotFoundError, PeerUnknownError
from src.common.messages import SearchResultPeer, SyncTableEntry
from src.tracker.index import Index

HASH_A = "a" * 64
HASH_B = "b" * 64


class RelogioFake:
    """Relógio determinístico injetado no Index (evita monkeypatch global)."""

    def __init__(self, inicio: float = 1000.0) -> None:
        self.agora = inicio

    def __call__(self) -> float:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += segundos


@pytest.fixture()
def relogio() -> RelogioFake:
    return RelogioFake()


@pytest.fixture()
def indice(relogio: RelogioFake) -> Index:
    return Index(clock=relogio)


def _hello_e_upload(indice: Index, nome_peer: str = "alice", porta: int = 7001) -> None:
    indice.register_peer(nome_peer, "127.0.0.1", porta)
    indice.register_file(
        nome_peer, HASH_A, nome="Imagine", tamanho=5_000_000, n_chunks=20
    )


# ---------------------------------------------------------------------------
# Presença
# ---------------------------------------------------------------------------


def test_register_peer_grava_endereco_e_last_seed(
    indice: Index, relogio: RelogioFake
) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    snapshot = indice.get_snapshot()
    endereco = snapshot.nome_peer_to_endereco["alice"]
    assert (endereco.ip, endereco.porta) == ("127.0.0.1", 7001)
    assert endereco.last_seed_ts == relogio.agora


def test_update_last_seed_avanca_timestamp(indice: Index, relogio: RelogioFake) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    relogio.avancar(180)
    indice.update_last_seed("alice")
    assert indice.get_snapshot().nome_peer_to_endereco["alice"].last_seed_ts == 1180.0


def test_update_last_seed_peer_desconhecido(indice: Index) -> None:
    with pytest.raises(PeerUnknownError):
        indice.update_last_seed("fantasma")


def test_remove_peer_some_do_indice_e_tombstona_fontes(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.remove_peer("alice")
    snapshot = indice.get_snapshot()
    assert "alice" not in snapshot.nome_peer_to_endereco
    assert "alice" in snapshot.tombstones[HASH_A]
    assert indice.search_by_name("Imagine") == []


def test_remove_peer_desconhecido(indice: Index) -> None:
    with pytest.raises(PeerUnknownError):
        indice.remove_peer("fantasma")


# ---------------------------------------------------------------------------
# Registro de arquivos e busca
# ---------------------------------------------------------------------------


def test_register_file_exige_peer_hello_antes(indice: Index) -> None:
    with pytest.raises(PeerUnknownError):
        indice.register_file("alice", HASH_A, nome="Imagine", tamanho=1, n_chunks=1)


def test_register_file_novo_hash_sem_metadados_falha(indice: Index) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    with pytest.raises(NotFoundError):
        indice.register_file("alice", HASH_A)  # upload inicial sem nome/tamanho


def test_upload_e_busca_por_nome_exato(indice: Index) -> None:
    _hello_e_upload(indice)
    resultados = indice.search_by_name("Imagine")
    assert len(resultados) == 1
    assert resultados[0].hash == HASH_A
    assert resultados[0].nome == "Imagine"
    assert resultados[0].peers == [
        SearchResultPeer(nome_peer="alice", ip="127.0.0.1", porta=7001)
    ]


def test_busca_nome_inexistente_retorna_vazio(indice: Index) -> None:
    assert indice.search_by_name("Inexistente") == []


def test_busca_por_stem_encontra_nome_com_extensao(indice: Index) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.register_file("alice", HASH_A, nome="Imagine.mp3", tamanho=1, n_chunks=1)

    por_stem = indice.search_by_name("Imagine")
    por_nome_completo = indice.search_by_name("Imagine.mp3")

    assert [r.hash for r in por_stem] == [HASH_A]
    assert [r.hash for r in por_nome_completo] == [HASH_A]
    assert por_stem[0].nome == "Imagine.mp3"


def test_mesmo_nome_pode_ter_multiplos_hashes(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.register_file("alice", HASH_B, nome="Imagine", tamanho=2, n_chunks=1)
    hashes = {r.hash for r in indice.search_by_name("Imagine")}
    assert hashes == {HASH_A, HASH_B}


def test_re_registro_sem_metadados_apos_download(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.register_peer("bob", "127.0.0.1", 7002)
    indice.register_file("bob", HASH_A)  # re-registro: metadados opcionais
    peers = {p.nome_peer for p in indice.get_peers_for_hash(HASH_A)}
    assert peers == {"alice", "bob"}


def test_get_peers_for_hash_desconhecido(indice: Index) -> None:
    with pytest.raises(NotFoundError):
        indice.get_peers_for_hash(HASH_A)


# ---------------------------------------------------------------------------
# Remoção e tombstones
# ---------------------------------------------------------------------------


def test_remove_peer_from_hash_vira_tombstone(
    indice: Index, relogio: RelogioFake
) -> None:
    _hello_e_upload(indice)
    relogio.avancar(60)
    indice.remove_peer_from_hash(HASH_A, "alice")
    snapshot = indice.get_snapshot()
    assert snapshot.tombstones[HASH_A]["alice"].timestamp == 1060.0
    assert indice.get_peers_for_hash(HASH_A) == []


def test_remove_peer_from_hash_nao_fonte_falha(indice: Index) -> None:
    _hello_e_upload(indice)
    with pytest.raises(NotFoundError):
        indice.remove_peer_from_hash(HASH_A, "bob")


def test_re_registro_reativa_tombstone(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.remove_peer_from_hash(HASH_A, "alice")
    indice.register_file("alice", HASH_A)
    snapshot = indice.get_snapshot()
    assert "alice" not in snapshot.tombstones.get(HASH_A, {})
    assert indice.get_peers_for_hash(HASH_A) != []


def test_outras_fontes_sobrevivem_a_remocao(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.register_peer("bob", "127.0.0.1", 7002)
    indice.register_file("bob", HASH_A)
    indice.remove_peer_from_hash(HASH_A, "alice")
    peers = {p.nome_peer for p in indice.get_peers_for_hash(HASH_A)}
    assert peers == {"bob"}


# ---------------------------------------------------------------------------
# UPDATE_IP e SEED_REPORT
# ---------------------------------------------------------------------------


def test_update_peer_address_reflete_nas_fontes(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.update_peer_address("alice", "10.0.0.5", 7099)
    assert indice.get_peers_for_hash(HASH_A) == [
        SearchResultPeer(nome_peer="alice", ip="10.0.0.5", porta=7099)
    ]


def test_update_peer_address_desconhecido(indice: Index) -> None:
    with pytest.raises(PeerUnknownError):
        indice.update_peer_address("fantasma", "10.0.0.5", 7099)


def test_apply_seed_hashes_remove_hash_omitido(indice: Index) -> None:
    # Omitir o hash no SEED_REPORT equivale a PEER_LEAVE_FILE (vira tombstone).
    _hello_e_upload(indice)
    indice.apply_seed_hashes("alice", set())
    assert indice.get_peers_for_hash(HASH_A) == []
    assert "alice" in indice.get_snapshot().tombstones[HASH_A]


def test_apply_seed_hashes_readiciona_hash_conhecido(indice: Index) -> None:
    _hello_e_upload(indice)
    indice.register_peer("bob", "127.0.0.1", 7002)
    indice.apply_seed_hashes("bob", {HASH_A})
    peers = {p.nome_peer for p in indice.get_peers_for_hash(HASH_A)}
    assert peers == {"alice", "bob"}


def test_apply_seed_hashes_ignora_hash_sem_metadata(indice: Index) -> None:
    indice.register_peer("alice", "127.0.0.1", 7001)
    indice.apply_seed_hashes("alice", {HASH_B})  # tracker nunca viu HASH_B
    assert HASH_B not in indice.get_snapshot().hash_to_peers


def test_apply_seed_hashes_preserva_timestamp_de_fonte_existente(
    indice: Index, relogio: RelogioFake
) -> None:
    _hello_e_upload(indice)
    relogio.avancar(180)
    indice.apply_seed_hashes("alice", {HASH_A})
    entry = indice.get_snapshot().hash_to_peers[HASH_A]["alice"]
    assert entry.timestamp == 1000.0  # sem churn de LWW a cada SEED_REPORT


# ---------------------------------------------------------------------------
# Snapshot e retornos para propagação via SYNC_TABLE
# ---------------------------------------------------------------------------


def test_snapshot_e_copia_independente(indice: Index) -> None:
    _hello_e_upload(indice)
    snapshot = indice.get_snapshot()
    snapshot.hash_to_peers[HASH_A].clear()
    assert indice.get_peers_for_hash(HASH_A) != []


def test_register_file_devolve_entrada_e_metadados(indice: Index) -> None:
    """O retorno alimenta o SYNC_TABLE com o MESMO timestamp local (LWW)."""
    indice.register_peer("alice", "127.0.0.1", 7001)
    entry, meta = indice.register_file(
        "alice", HASH_A, nome="Imagine", tamanho=5_000_000, n_chunks=20
    )
    assert (entry.nome_peer, entry.ip, entry.porta) == ("alice", "127.0.0.1", 7001)
    assert entry.timestamp == 1000.0
    assert (meta.nome, meta.tamanho, meta.n_chunks) == ("Imagine", 5_000_000, 20)


def test_remove_peer_from_hash_devolve_tombstone(
    indice: Index, relogio: RelogioFake
) -> None:
    _hello_e_upload(indice)
    relogio.avancar(30)
    tombstone = indice.remove_peer_from_hash(HASH_A, "alice")
    assert tombstone.nome_peer == "alice"
    assert tombstone.timestamp == 1030.0


def test_apply_sync_entry_registra_fonte_remota(indice: Index) -> None:
    entry = SyncTableEntry(
        hash=HASH_A,
        nome_peer="bob",
        ip="127.0.0.1",
        porta=7002,
        ativo=True,
        nome="Imagine",
        tamanho=5_000_000,
        n_chunks=20,
    )
    assert indice.apply_sync_entry(
        entry, origem_tracker="tracker-2", timestamp=900.0, seq=1
    )
    fontes = indice.get_peers_for_hash(HASH_A)
    assert [(p.nome_peer, p.ip, p.porta) for p in fontes] == [
        ("bob", "127.0.0.1", 7002)
    ]
    # Metadados da entry permitem busca por nome local no tracker que recebe.
    assert indice.search_by_name("Imagine")[0].hash == HASH_A
    # Presença NÃO é tocada: bob reporta SEED_REPORT ao tracker dele.
    assert "bob" not in indice.get_snapshot().nome_peer_to_endereco
