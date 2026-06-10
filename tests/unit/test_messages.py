"""Testes unitários para src/common/messages.py."""

from __future__ import annotations

import pytest

from pydantic import BaseModel

from src.common.messages import (
    MESSAGE_MODELS,
    REQUIRED_FIELDS,
    PeerHello,
    validate_message,
)


def test_validate_peer_hello_completo() -> None:
    msg = {"type": "PEER_HELLO", "nome_peer": "alice", "ip": "127.0.0.1", "porta": 7001}
    assert validate_message(msg) == "PEER_HELLO"


def test_validate_search_result_lista_vazia_eh_valida() -> None:
    msg = {"type": "SEARCH_RESULT", "query_id": "uuid-1", "resultados": []}
    assert validate_message(msg) == "SEARCH_RESULT"


def test_validate_chunk_data_header() -> None:
    msg = {
        "type": "CHUNK_DATA",
        "hash": "a" * 64,
        "chunk_index": 0,
        "payload_bytes": 262144,
    }
    assert validate_message(msg) == "CHUNK_DATA"


def test_validate_register_file_apenas_obrigatorios() -> None:
    """nome/tamanho/n_chunks são opcionais ao re-registrar."""
    msg = {"type": "REGISTER_FILE", "nome_peer": "alice", "hash": "h" * 64}
    assert validate_message(msg) == "REGISTER_FILE"


def test_validate_error_message() -> None:
    msg = {
        "type": "ERROR",
        "ref_type": "CHUNK_REQUEST",
        "ref_id": "q-1",
        "code": "NOT_FOUND",
        "mensagem": "chunk inexistente",
    }
    assert validate_message(msg) == "ERROR"


def test_validate_missing_type() -> None:
    with pytest.raises(ValueError, match="type"):
        validate_message({"nome_peer": "alice"})


def test_validate_unknown_type() -> None:
    with pytest.raises(ValueError, match="desconhecido"):
        validate_message({"type": "FOO_BAR"})


def test_validate_missing_required_field() -> None:
    with pytest.raises(ValueError, match="obrigatórios"):
        validate_message({"type": "PEER_HELLO", "nome_peer": "alice"})


def test_validate_msg_nao_dict() -> None:
    with pytest.raises(ValueError, match="dict"):
        validate_message([1, 2, 3])  # type: ignore[arg-type]


def test_validate_campo_com_tipo_errado() -> None:
    msg = {"type": "PEER_HELLO", "nome_peer": "alice", "ip": "127.0.0.1", "porta": "x"}
    with pytest.raises(ValueError, match="inválida"):
        validate_message(msg)


def test_required_fields_cobre_todos_os_tipos() -> None:
    """Sanity: todos os 19 tipos do Listing 7.2 estão em REQUIRED_FIELDS."""
    esperados = {
        "PEER_HELLO", "PEER_LEAVE", "UPDATE_IP", "SEED_REPORT",
        "REGISTER_FILE", "SEARCH_FILE", "SEARCH_FORWARD", "SEARCH_RESULT",
        "CHUNK_LIST_REQUEST", "CHUNK_LIST", "CHUNK_REQUEST", "CHUNK_DATA",
        "PEER_LEAVE_FILE",
        "SYNC_TABLE", "FULL_SYNC",
        "TRACKER_REJOIN", "TRACKER_ANNOUNCE", "REASSIGN_TRACKER",
        "ERROR",
    }
    assert set(REQUIRED_FIELDS.keys()) == esperados
    assert set(MESSAGE_MODELS.keys()) == esperados


def test_modelos_sao_pydantic_com_type_default() -> None:
    """Cada modelo é BaseModel e serializa o campo ``type`` correto."""
    for tipo, modelo in MESSAGE_MODELS.items():
        assert issubclass(modelo, BaseModel)
        assert modelo.model_fields["type"].default == tipo


def test_modelo_serializa_para_dict_do_protocolo() -> None:
    hello = PeerHello(nome_peer="alice", ip="127.0.0.1", porta=7001)
    assert hello.model_dump() == {
        "type": "PEER_HELLO",
        "nome_peer": "alice",
        "ip": "127.0.0.1",
        "porta": 7001,
    }
