"""Testes do PlaylistStore (playlists locais do peer, persistidas em JSON)."""

from __future__ import annotations

from pathlib import Path

from src.peer.playlist_store import PlaylistStore

HASH_A = "a" * 64
HASH_B = "b" * 64


def test_criar_listar_e_obter(tmp_path: Path) -> None:
    store = PlaylistStore(tmp_path)
    pid = store.criar("favoritas")
    assert store.listar() == [{"id": pid, "nome": "favoritas"}]
    assert store.obter(pid) == {"nome": "favoritas", "itens": []}


def test_ids_monotonicos_sem_reuso(tmp_path: Path) -> None:
    store = PlaylistStore(tmp_path)
    p1 = store.criar("a")
    p2 = store.criar("b")
    assert p2 == p1 + 1
    store.deletar(p2)
    # Após deletar, um novo id não reutiliza o anterior.
    assert store.criar("c") == p2 + 1


def test_adicionar_e_remover_itens(tmp_path: Path) -> None:
    store = PlaylistStore(tmp_path)
    pid = store.criar("mix")
    assert store.adicionar_item(pid, HASH_A) is True
    assert store.adicionar_item(pid, HASH_B) is True
    assert store.adicionar_item(pid, HASH_A) is True  # duplicata é ignorada
    assert store.obter(pid)["itens"] == [HASH_A, HASH_B]
    assert store.remover_item(pid, HASH_A) is True
    assert store.obter(pid)["itens"] == [HASH_B]


def test_operacoes_em_playlist_inexistente(tmp_path: Path) -> None:
    store = PlaylistStore(tmp_path)
    assert store.obter(999) is None
    assert store.adicionar_item(999, HASH_A) is False
    assert store.remover_item(999, HASH_A) is False
    assert store.deletar(999) is False


def test_persiste_entre_instancias(tmp_path: Path) -> None:
    # O ponto do pedido: playlists sobrevivem mesmo sem tracker (estado local).
    store = PlaylistStore(tmp_path)
    pid = store.criar("favoritas")
    store.adicionar_item(pid, HASH_A)

    outra = PlaylistStore(tmp_path)  # nova instância relê o disco
    assert outra.listar() == [{"id": pid, "nome": "favoritas"}]
    assert outra.obter(pid) == {"nome": "favoritas", "itens": [HASH_A]}
    # O contador de ids também persiste (não reinicia em 1).
    assert outra.criar("nova") == pid + 1


def test_json_corrompido_comeca_vazio(tmp_path: Path) -> None:
    (tmp_path / "playlists.json").write_text("{ inválido", encoding="utf-8")
    store = PlaylistStore(tmp_path)
    assert store.listar() == []
    assert store.criar("nova") == 1  # segue utilizável após reset
