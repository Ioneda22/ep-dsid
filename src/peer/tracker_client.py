"""Cliente REST do peer para o tracker.

Fase 3: SEM fallback — usa apenas o primeiro tracker da lista ``trackers``
do YAML. O fallback completo (timeout/ConnectionRefused → próximo da
lista, §7.5) chega na Fase 5.

Erros de comunicação não sobem como exceção: cada método loga e retorna
``None``, deixando a CLI decidir a mensagem ao usuário.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from pydantic import ValidationError

from src.common.messages import (
    PeerHello,
    PeerLeave,
    PeerLeaveFile,
    RegisterFile,
    SearchFile,
    SearchResult,
    SeedReport,
)

logger = logging.getLogger(__name__)


class PeerTrackerClient:
    """Cliente HTTP síncrono do peer para a API REST do tracker."""

    def __init__(self, trackers: list[dict[str, Any]], timeout: float = 10.0) -> None:
        """Inicializa o cliente apontando para o primeiro tracker da lista.

        Args:
            trackers: Lista ``trackers`` do YAML do peer; cada item tem
                ``tracker_id``, ``ip`` e ``api_port``. Apenas o primeiro
                é usado nesta fase.
            timeout: Timeout em segundos para cada requisição.

        Raises:
            ValueError: Se ``trackers`` estiver vazia.
        """
        if not trackers:
            raise ValueError("lista de trackers vazia; esperado ao menos um")
        primeiro = trackers[0]
        base_url = f"http://{primeiro['ip']}:{primeiro['api_port']}"
        self.tracker_id = str(primeiro.get("tracker_id", base_url))
        self._http = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        """Encerra a sessão HTTP subjacente."""
        self._http.close()

    # ------------------------------------------------------------------
    # Presença
    # ------------------------------------------------------------------

    def peer_hello(self, nome_peer: str, ip: str, porta: int) -> dict[str, Any] | None:
        """Envia ``PEER_HELLO`` (apresentação inicial, §7.2 do main.tex)."""
        corpo = PeerHello(nome_peer=nome_peer, ip=ip, porta=porta)
        return self._post("/peers/hello", corpo.model_dump())

    def peer_leave(self, nome_peer: str) -> dict[str, Any] | None:
        """Envia ``PEER_LEAVE`` (saída ordenada)."""
        corpo = PeerLeave(nome_peer=nome_peer)
        return self._post("/peers/leave", corpo.model_dump())

    def seed_report(
        self, nome_peer: str, ip: str, porta: int, hashes: list[str]
    ) -> dict[str, Any] | None:
        """Envia ``SEED_REPORT`` com os hashes completos deste peer."""
        corpo = SeedReport(nome_peer=nome_peer, ip=ip, porta=porta, hashes=hashes)
        return self._post("/peers/seed-report", corpo.model_dump())

    # ------------------------------------------------------------------
    # Registro e busca
    # ------------------------------------------------------------------

    def register_file(
        self,
        nome_peer: str,
        hash_arquivo: str,
        nome: str | None = None,
        tamanho: int | None = None,
        n_chunks: int | None = None,
    ) -> dict[str, Any] | None:
        """Envia ``REGISTER_FILE`` (upload original ou re-registro).

        No re-registro pós-download, ``nome``/``tamanho``/``n_chunks``
        ficam ``None`` — o tracker já os conhece (main.tex §7.2).
        """
        corpo = RegisterFile(
            nome_peer=nome_peer,
            hash=hash_arquivo,
            nome=nome,
            tamanho=tamanho,
            n_chunks=n_chunks,
        )
        return self._post("/files/register", corpo.model_dump(exclude_none=True))

    def search_file(self, query: str, query_id: str) -> SearchResult | None:
        """Envia ``SEARCH_FILE`` e devolve o ``SEARCH_RESULT`` tipado.

        Args:
            query: Nome legível da música.
            query_id: UUID gerado pelo peer (correlação, main.tex §7.2).
        """
        corpo = SearchFile(query_id=query_id, query=query, ttl=3)
        resposta = self._post("/search", corpo.model_dump())
        if resposta is None:
            return None
        try:
            return SearchResult.model_validate(resposta)
        except ValidationError:
            logger.exception("SEARCH_RESULT inválido do tracker: %r", resposta)
            return None

    def peer_leave_file(
        self, nome_peer: str, hash_arquivo: str
    ) -> dict[str, Any] | None:
        """Envia ``PEER_LEAVE_FILE`` (remoção imediata de uma fonte)."""
        corpo = PeerLeaveFile(nome_peer=nome_peer, hash=hash_arquivo)
        return self._post("/files/leave", corpo.model_dump())

    # ------------------------------------------------------------------
    # Transporte
    # ------------------------------------------------------------------

    def _post(self, rota: str, corpo: dict[str, Any]) -> dict[str, Any] | None:
        """POST na API do tracker; ``None`` em qualquer falha (já logada)."""
        try:
            resposta = self._http.post(rota, json=corpo)
            resposta.raise_for_status()
            return resposta.json()
        except httpx.TimeoutException:
            logger.error("timeout no tracker %s rota=%s", self.tracker_id, rota)
            return None
        except httpx.ConnectError:
            logger.error(
                "conexão recusada pelo tracker %s rota=%s", self.tracker_id, rota
            )
            return None
        except httpx.HTTPStatusError as exc:
            logger.error(
                "tracker %s rota=%s HTTP %d: %s",
                self.tracker_id,
                rota,
                exc.response.status_code,
                exc.response.text,
            )
            return None
        except httpx.HTTPError:
            logger.exception("erro HTTP no tracker %s rota=%s", self.tracker_id, rota)
            return None
