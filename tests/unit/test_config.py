"""Testes unitários para src/common/config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.config import ConfigError, load_yaml, require_keys


def test_load_yaml_basico(tmp_path: Path) -> None:
    """Um YAML válido com raiz de mapeamento é lido como dict com os tipos preservados (str e int)."""
    f = tmp_path / "c.yaml"
    f.write_text("tracker_id: tracker-1\nporta: 8001\n", encoding="utf-8")
    assert load_yaml(f) == {"tracker_id": "tracker-1", "porta": 8001}


def test_load_yaml_arquivo_inexistente(tmp_path: Path) -> None:
    """Apontar para um caminho que não existe deve levantar ConfigError sinalizando arquivo não encontrado."""
    with pytest.raises(ConfigError, match="não encontrado"):
        load_yaml(tmp_path / "nao-existe.yaml")


def test_load_yaml_vazio(tmp_path: Path) -> None:
    """Um arquivo existente porém sem conteúdo deve levantar ConfigError indicando YAML vazio."""
    f = tmp_path / "vazio.yaml"
    f.write_text("", encoding="utf-8")
    with pytest.raises(ConfigError, match="vazio"):
        load_yaml(f)


def test_load_yaml_raiz_nao_mapeamento(tmp_path: Path) -> None:
    """Uma raiz que é lista (e não mapeamento chave/valor) deve levantar ConfigError exigindo mapeamento."""
    f = tmp_path / "lista.yaml"
    f.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="mapeamento"):
        load_yaml(f)


def test_load_yaml_invalido(tmp_path: Path) -> None:
    """Sintaxe YAML mal-formada deve virar ConfigError que menciona o erro de parsing YAML, não propagar a exceção crua."""
    f = tmp_path / "bad.yaml"
    f.write_text("a: [b: c", encoding="utf-8")  # YAML mal-formado
    with pytest.raises(ConfigError, match="YAML"):
        load_yaml(f)


def test_require_keys_todas_presentes() -> None:
    """Quando todas as chaves obrigatórias estão presentes, require_keys retorna sem levantar exceção."""
    require_keys({"a": 1, "b": 2}, ["a", "b"], "origem")


def test_require_keys_faltando() -> None:
    """Chaves obrigatórias ausentes devem levantar ConfigError apontando as que faltam na origem informada."""
    with pytest.raises(ConfigError, match="obrigatórias"):
        require_keys({"a": 1}, ["a", "b", "c"], "origem-x")
