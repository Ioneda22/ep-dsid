"""Testes unitários para src/common/config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.config import ConfigError, load_yaml, require_keys


def test_load_yaml_basico(tmp_path: Path) -> None:
    f = tmp_path / "c.yaml"
    f.write_text("tracker_id: tracker-1\nporta: 8001\n", encoding="utf-8")
    assert load_yaml(f) == {"tracker_id": "tracker-1", "porta": 8001}


def test_load_yaml_arquivo_inexistente(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="não encontrado"):
        load_yaml(tmp_path / "nao-existe.yaml")


def test_load_yaml_vazio(tmp_path: Path) -> None:
    f = tmp_path / "vazio.yaml"
    f.write_text("", encoding="utf-8")
    with pytest.raises(ConfigError, match="vazio"):
        load_yaml(f)


def test_load_yaml_raiz_nao_mapeamento(tmp_path: Path) -> None:
    f = tmp_path / "lista.yaml"
    f.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="mapeamento"):
        load_yaml(f)


def test_load_yaml_invalido(tmp_path: Path) -> None:
    f = tmp_path / "bad.yaml"
    f.write_text("a: [b: c", encoding="utf-8")  # YAML mal-formado
    with pytest.raises(ConfigError, match="YAML"):
        load_yaml(f)


def test_require_keys_todas_presentes() -> None:
    require_keys({"a": 1, "b": 2}, ["a", "b"], "origem")


def test_require_keys_faltando() -> None:
    with pytest.raises(ConfigError, match="obrigatórias"):
        require_keys({"a": 1}, ["a", "b", "c"], "origem-x")
