"""Códigos de erro e exceções customizadas do PeerSpot.

Os códigos abaixo são usados no campo ``code`` da mensagem ``ERROR``
(Listing 7.2 do ``main.tex``). Mantenha-os como constantes para evitar
typos. As exceções correspondentes têm o atributo ``code`` apontando para
a constante.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Códigos de erro (campo ``code`` da mensagem ERROR)
# ---------------------------------------------------------------------------

NOT_FOUND = "NOT_FOUND"
INVALID_HASH = "INVALID_HASH"
INVALID_CHUNK = "INVALID_CHUNK"
UNAUTHORIZED = "UNAUTHORIZED"
PEER_UNKNOWN = "PEER_UNKNOWN"
TRACKER_UNKNOWN = "TRACKER_UNKNOWN"
MALFORMED_MESSAGE = "MALFORMED_MESSAGE"
INTERNAL_ERROR = "INTERNAL_ERROR"

#: Conjunto de todos os códigos válidos, útil em validações.
ERROR_CODES: frozenset[str] = frozenset(
    {
        NOT_FOUND,
        INVALID_HASH,
        INVALID_CHUNK,
        UNAUTHORIZED,
        PEER_UNKNOWN,
        TRACKER_UNKNOWN,
        MALFORMED_MESSAGE,
        INTERNAL_ERROR,
    }
)


# ---------------------------------------------------------------------------
# Hierarquia de exceções
# ---------------------------------------------------------------------------


class PeerSpotError(Exception):
    """Classe base de todas as exceções do PeerSpot.

    Attributes:
        code: Código de erro correspondente (constantes deste módulo).
    """

    code: str = INTERNAL_ERROR

    def __init__(self, mensagem: str = "", *, code: str | None = None) -> None:
        super().__init__(mensagem)
        if code is not None:
            self.code = code


class NotFoundError(PeerSpotError):
    """Recurso (hash, peer, chunk) não encontrado no índice ou no peer."""

    code = NOT_FOUND


class InvalidHashError(PeerSpotError):
    """Hash inválido (formato ou conteúdo divergente do esperado)."""

    code = INVALID_HASH


class InvalidChunkError(PeerSpotError):
    """Chunk inválido (índice fora de faixa ou tamanho divergente)."""

    code = INVALID_CHUNK


class UnauthorizedError(PeerSpotError):
    """Operação não autorizada para o peer/tracker requerente."""

    code = UNAUTHORIZED


class PeerUnknownError(PeerSpotError):
    """Peer mencionado não está registrado no tracker."""

    code = PEER_UNKNOWN


class TrackerUnknownError(PeerSpotError):
    """Tracker mencionado não está na lista conhecida."""

    code = TRACKER_UNKNOWN


class MalformedMessageError(PeerSpotError):
    """Mensagem mal-formada (campos faltando, tipos errados, etc.)."""

    code = MALFORMED_MESSAGE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_error_message(
    ref_type: str, ref_id: str, code: str, mensagem: str
) -> dict[str, Any]:
    """Constrói um dicionário ``ERROR`` pronto para serialização.

    Args:
        ref_type: Tipo da mensagem que falhou (ex: ``"CHUNK_REQUEST"``).
        ref_id: ``query_id`` ou outro correlator; ``""`` se não aplicável.
        code: Um dos códigos definidos neste módulo.
        mensagem: Descrição legível para depuração.

    Returns:
        Dicionário com a estrutura de ``ERROR`` (Listing 7.2).

    Raises:
        ValueError: Se ``code`` não for um código válido.
    """
    if code not in ERROR_CODES:
        raise ValueError(
            f"Código de erro desconhecido: {code!r} "
            f"(esperado um de {sorted(ERROR_CODES)})"
        )
    return {
        "type": "ERROR",
        "ref_type": ref_type,
        "ref_id": ref_id,
        "code": code,
        "mensagem": mensagem,
    }
