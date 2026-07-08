"""Camada REST do tracker — FastAPI (§6.1, camada 1).

Os corpos das rotas são os próprios modelos pydantic do Listing 7.2
(``src.common.messages``) — uma única fonte de verdade para schema e
validação. Cada rota delega ao handler correspondente em
``src.tracker.handlers``. As rotas são ``def`` síncronas de propósito:
o uvicorn as despacha num threadpool, o que combina com o
``threading.Lock`` do ``Index`` sem bloquear o event loop.

Erros de domínio (``PeerSpotError``) viram mensagens ``ERROR`` do
protocolo com status HTTP correspondente.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.common import errors
from src.common.errors import PeerSpotError, build_error_message
from src.common.messages import (
    PeerHello,
    PeerLeave,
    PeerLeaveFile,
    RegisterFile,
    SearchFile,
    SearchResult,
    SeedReport,
    UpdateIp,
)
from src.tracker import handlers
from src.tracker.index import Index
from src.tracker.persistence import TrackerDB
from src.tracker.routing import SearchRouter
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)

#: Status HTTP por código de ERROR do protocolo.
_HTTP_STATUS_POR_CODIGO: dict[str, int] = {
    errors.NOT_FOUND: 404,
    errors.PEER_UNKNOWN: 404,
    errors.TRACKER_UNKNOWN: 404,
    errors.INVALID_HASH: 400,
    errors.INVALID_CHUNK: 400,
    errors.MALFORMED_MESSAGE: 400,
    errors.UNAUTHORIZED: 403,
    errors.INTERNAL_ERROR: 500,
}

#: ``ref_type`` da mensagem ERROR, derivado da rota que falhou.
_REF_TYPE_POR_ROTA: dict[str, str] = {
    "/peers/hello": "PEER_HELLO",
    "/peers/leave": "PEER_LEAVE",
    "/peers/update-ip": "UPDATE_IP",
    "/peers/seed-report": "SEED_REPORT",
    "/files/register": "REGISTER_FILE",
    "/files/leave": "PEER_LEAVE_FILE",
    "/search": "SEARCH_FILE",
}


def create_app(
    index: Index,
    db: TrackerDB,
    tracker_id: str,
    trackers_conhecidos: list[dict[str, Any]],
    sync_client: SyncClient | None = None,
    search_router: SearchRouter | None = None,
) -> FastAPI:
    """Monta o app FastAPI do tracker com dependências injetadas (§14.4).

    Args:
        index: Índice em memória compartilhado do tracker.
        db: Persistência SQLite (usuários, playlists).
        tracker_id: Identificador deste tracker (ex.: ``"tracker-1"``).
        trackers_conhecidos: Lista de trackers (incluindo este) exposta em
            ``GET /trackers`` — espelha o ``trackers_conhecidos`` do
            ``TRACKER_LIST`` (membership da reintegração).
        sync_client: Flooding ``SYNC_TABLE`` aos demais trackers (Fase 4);
            ``None`` desliga a propagação (tracker isolado/testes).
        search_router: Roteamento ``SEARCH_FORWARD`` quando a busca local
            não tem hit (Fase 4); ``None`` limita a busca ao índice local.

    Returns:
        App pronto para ser servido pelo uvicorn.
    """
    app = FastAPI(title=f"PeerSpot Tracker {tracker_id}")

    @app.exception_handler(PeerSpotError)
    async def _erro_dominio(request: Request, exc: PeerSpotError) -> JSONResponse:
        ref_type = _REF_TYPE_POR_ROTA.get(request.url.path, "")
        logger.warning(
            "tracker_id=%s rota=%s erro=%s: %s",
            tracker_id,
            request.url.path,
            exc.code,
            exc,
        )
        return JSONResponse(
            status_code=_HTTP_STATUS_POR_CODIGO.get(exc.code, 500),
            content=build_error_message(ref_type, "", exc.code, str(exc)),
        )

    @app.exception_handler(RequestValidationError)
    async def _erro_validacao(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        ref_type = _REF_TYPE_POR_ROTA.get(request.url.path, "")
        logger.warning(
            "tracker_id=%s rota=%s payload malformado: %s",
            tracker_id,
            request.url.path,
            exc.errors(),
        )
        return JSONResponse(
            status_code=400,
            content=build_error_message(
                ref_type, "", errors.MALFORMED_MESSAGE, str(exc.errors())
            ),
        )

    @app.post("/peers/hello")
    def peers_hello(body: PeerHello) -> dict[str, Any]:
        return handlers.handle_peer_hello(body, index, db)

    @app.post("/peers/leave")
    def peers_leave(body: PeerLeave) -> dict[str, Any]:
        return handlers.handle_peer_leave(body, index, sync_client)

    @app.post("/peers/update-ip")
    def peers_update_ip(body: UpdateIp) -> dict[str, Any]:
        return handlers.handle_update_ip(body, index)

    @app.post("/peers/seed-report")
    def peers_seed_report(body: SeedReport) -> dict[str, Any]:
        return handlers.handle_seed_report(body, index, sync_client)

    @app.post("/files/register")
    def files_register(body: RegisterFile) -> dict[str, Any]:
        return handlers.handle_register_file(body, index, sync_client)

    @app.post("/files/leave")
    def files_leave(body: PeerLeaveFile) -> dict[str, Any]:
        return handlers.handle_peer_leave_file(body, index, sync_client)

    @app.post("/search")
    def search(body: SearchFile) -> SearchResult:
        return handlers.handle_search_file(body, index, search_router)

    @app.get("/trackers")
    def trackers() -> dict[str, list[dict[str, Any]]]:
        return {"trackers": trackers_conhecidos}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "tracker_id": tracker_id}

    return app
