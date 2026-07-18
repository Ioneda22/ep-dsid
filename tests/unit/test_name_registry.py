"""Testes do NameRegistry (mapa hash↔nome persistido do peer)."""

from __future__ import annotations

from pathlib import Path

from src.peer.name_registry import NameRegistry

HASH_A = "a" * 64
HASH_B = "b" * 64


def test_registrar_e_consultar(tmp_path: Path) -> None:
    reg = NameRegistry(tmp_path)
    reg.registrar(HASH_A, "Imagine")
    assert reg.nome(HASH_A) == "Imagine"
    assert reg.nome(HASH_B) is None


def test_persiste_entre_instancias(tmp_path: Path) -> None:
    NameRegistry(tmp_path).registrar(HASH_A, "Imagine")
    # Uma nova instância no mesmo diretório relê o JSON do disco.
    assert NameRegistry(tmp_path).nome(HASH_A) == "Imagine"


def test_esquecer_remove_e_persiste(tmp_path: Path) -> None:
    reg = NameRegistry(tmp_path)
    reg.registrar(HASH_A, "Imagine")
    reg.esquecer(HASH_A)
    assert reg.nome(HASH_A) is None
    assert NameRegistry(tmp_path).nome(HASH_A) is None


def test_esquecer_hash_inexistente_e_noop(tmp_path: Path) -> None:
    reg = NameRegistry(tmp_path)
    reg.esquecer(HASH_A)  # não deve levantar
    assert reg.nome(HASH_A) is None


def test_hashes_por_nome_case_insensitive(tmp_path: Path) -> None:
    reg = NameRegistry(tmp_path)
    reg.registrar(HASH_A, "Imagine")
    reg.registrar(HASH_B, "Imagine")  # mesmo nome, hashes distintos
    assert set(reg.hashes_por_nome("imagine")) == {HASH_A, HASH_B}
    assert reg.hashes_por_nome("outra") == []


def test_json_corrompido_comeca_vazio(tmp_path: Path) -> None:
    (tmp_path / "nomes.json").write_text("{ inválido", encoding="utf-8")
    reg = NameRegistry(tmp_path)
    assert reg.nome(HASH_A) is None
    reg.registrar(HASH_A, "Nova")  # segue utilizável após reset
    assert reg.nome(HASH_A) == "Nova"
