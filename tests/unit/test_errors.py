"""Testes unitários para src/common/errors.py."""

from __future__ import annotations

import pytest

from src.common import errors


def test_codigos_de_erro_definidos() -> None:
    esperados = {
        "NOT_FOUND",
        "INVALID_HASH",
        "INVALID_CHUNK",
        "UNAUTHORIZED",
        "PEER_UNKNOWN",
        "TRACKER_UNKNOWN",
        "MALFORMED_MESSAGE",
        "INTERNAL_ERROR",
    }
    assert errors.ERROR_CODES == esperados


def test_build_error_message_estrutura() -> None:
    msg = errors.build_error_message(
        ref_type="CHUNK_REQUEST",
        ref_id="q-1",
        code=errors.NOT_FOUND,
        mensagem="chunk inexistente",
    )
    assert msg == {
        "type": "ERROR",
        "ref_type": "CHUNK_REQUEST",
        "ref_id": "q-1",
        "code": "NOT_FOUND",
        "mensagem": "chunk inexistente",
    }


def test_build_error_message_codigo_invalido() -> None:
    with pytest.raises(ValueError, match="desconhecido"):
        errors.build_error_message("X", "y", "FOO", "bar")


def test_excecoes_carregam_codigo_correto() -> None:
    assert errors.NotFoundError().code == errors.NOT_FOUND
    assert errors.InvalidHashError().code == errors.INVALID_HASH
    assert errors.InvalidChunkError().code == errors.INVALID_CHUNK
    assert errors.UnauthorizedError().code == errors.UNAUTHORIZED
    assert errors.PeerUnknownError().code == errors.PEER_UNKNOWN
    assert errors.TrackerUnknownError().code == errors.TRACKER_UNKNOWN
    assert errors.MalformedMessageError().code == errors.MALFORMED_MESSAGE
    assert errors.PeerSpotError().code == errors.INTERNAL_ERROR


def test_excecao_aceita_override_de_codigo() -> None:
    exc = errors.PeerSpotError("oops", code=errors.UNAUTHORIZED)
    assert exc.code == errors.UNAUTHORIZED
    assert str(exc) == "oops"
