"""Cliente REST do peer para o tracker, com fallback completo.

O peer mantém a lista ordenada trackers do YAML e um current_tracker_index
que NÃO reseta a cada chamada. Em timeout/ConnectionRefused do tracker atual, o
cliente avança para o próximo da lista (e, por ser uma troca de tracker, reenvia
PEER_HELLO ao novo antes de retomar a operação). Se TODOS falharem, levanta
TodosTrackersIndisponiveis — a CLI mostra o erro ao usuário.

Rebalance: quando a resposta de uma chamada traz reassign_to,
o cliente migra para o tracker indicado (adicionando-o à lista se necessário) e se
reapresenta lá — simplificação aceita para entregar o REASSIGN_TRACKER sobre
REST, sem um canal push tracker→peer.

Erros que NÃO são de conectividade (HTTP 4xx/5xx) continuam retornando None:
cada método loga e devolve None, deixando a camada acima decidir a mensagem.
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


class TodosTrackersIndisponiveis(Exception):
    """Nenhum tracker da lista de fallback respondeu."""


class PeerTrackerClient:
    """Cliente HTTP síncrono do peer para a API REST do tracker, com fallback."""

    def __init__(self, trackers: list[dict[str, Any]], timeout: float = 10.0) -> None:
        """Inicializa o cliente apontando para o primeiro tracker da lista.

        Args:
            trackers: Lista trackers do YAML do peer; cada item tem
                tracker_id, ip e api_port.
            timeout: Timeout em segundos para cada requisição.

        Raises:
            ValueError: Se trackers estiver vazia.
        """
        if not trackers:
            raise ValueError("lista de trackers vazia; esperado ao menos um")
        self._trackers: list[dict[str, Any]] = [dict(t) for t in trackers]
        self.timeout = timeout
        self.current_tracker_index = 0
        self._clientes: dict[int, httpx.Client] = {}
        # Identidade guardada no primeiro PEER_HELLO, para reapresentar o peer
        # ao trocar de tracker (fallback ou reassign) sem intervenção externa.
        self._identidade: tuple[str, str, int] | None = None
        self._reassignando = False

    @property
    def tracker_id(self) -> str:
        """tracker_id do tracker atualmente em uso."""
        atual = self._trackers[self.current_tracker_index]
        return str(atual.get("tracker_id", self._base_url(self.current_tracker_index)))

    @property
    def trackers_conhecidos(self) -> list[str]:
        """Ids (ou URLs) de todos os trackers da lista de fallback (usado no status)."""
        return [self._id(i) for i in range(len(self._trackers))]

    @property
    def tracker_endereco(self) -> str:
        """Endereço ip:api_port do tracker atualmente em uso (usado no status)."""
        atual = self._trackers[self.current_tracker_index]
        return f"{atual['ip']}:{atual['api_port']}"

    def health(self) -> bool:
        """GET /health no tracker ATUAL (sem fallback); True se responder ok.

        Usado pelo comando status para mostrar ✓/✗ do tracker corrente; não
        deve migrar de tracker só por checar saúde, então ignora o fallback.
        """
        try:
            resposta = self._cliente(self.current_tracker_index).get("/health")
            resposta.raise_for_status()
            return True
        except httpx.HTTPError:
            return False

    def close(self) -> None:
        """Encerra todas as sessões HTTP abertas."""
        for cliente in self._clientes.values():
            cliente.close()
        self._clientes.clear()

    # ------------------------------------------------------------------
    # Presença
    # ------------------------------------------------------------------

    def peer_hello(self, nome_peer: str, ip: str, porta: int) -> dict[str, Any] | None:
        """Envia PEER_HELLO e memoriza a identidade para futuros fallbacks."""
        self._identidade = (nome_peer, ip, porta)
        corpo = PeerHello(nome_peer=nome_peer, ip=ip, porta=porta)
        return self._post("/peers/hello", corpo.model_dump())

    def reenviar_hello(self) -> bool:
        """Reapresenta o peer (PEER_HELLO) usando a identidade já guardada.

        Usado na recuperação de um tracker que perdeu o estado: ele também
        esqueceu a presença do peer, então um REGISTER_FILE falharia com
        PEER_UNKNOWN — um hello prévio restabelece a presença. No-op (False) se
        o peer ainda não fez o primeiro hello.
        """
        if self._identidade is None:
            return False
        nome_peer, ip, porta = self._identidade
        return self.peer_hello(nome_peer, ip, porta) is not None

    def peer_leave(self, nome_peer: str) -> dict[str, Any] | None:
        """Envia PEER_LEAVE (saída ordenada)."""
        corpo = PeerLeave(nome_peer=nome_peer)
        return self._post("/peers/leave", corpo.model_dump())

    def seed_report(
        self, nome_peer: str, ip: str, porta: int, hashes: list[str]
    ) -> dict[str, Any] | None:
        """Envia SEED_REPORT com os hashes completos deste peer."""
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
        """Envia REGISTER_FILE (upload original ou re-registro)."""
        corpo = RegisterFile(
            nome_peer=nome_peer,
            hash=hash_arquivo,
            nome=nome,
            tamanho=tamanho,
            n_chunks=n_chunks,
        )
        return self._post("/files/register", corpo.model_dump(exclude_none=True))

    def search_file(self, query: str, query_id: str) -> SearchResult | None:
        """Envia SEARCH_FILE e devolve o SEARCH_RESULT tipado."""
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
        """Envia PEER_LEAVE_FILE (remoção imediata de uma fonte)."""
        corpo = PeerLeaveFile(nome_peer=nome_peer, hash=hash_arquivo)
        return self._post("/files/leave", corpo.model_dump())

    # ------------------------------------------------------------------
    # Playlists — dados de usuário, locais ao tracker atual
    # ------------------------------------------------------------------

    def criar_playlist(self, dono: str, nome: str) -> int | None:
        """POST /playlists; devolve o playlist_id criado."""
        resposta = self._post("/playlists", {"dono": dono, "nome": nome})
        if resposta is None:
            return None
        return int(resposta["playlist_id"])

    def listar_playlists(self, dono: str) -> list[dict[str, Any]] | None:
        """GET /playlists/{dono}; devolve a lista de playlists do dono."""
        resposta = self._get(f"/playlists/{dono}")
        if resposta is None:
            return None
        return list(resposta["playlists"])

    def obter_playlist(self, playlist_id: int) -> dict[str, Any] | None:
        """GET /playlists/{id}; devolve {nome, dono, itens} ou None."""
        return self._get(f"/playlists/{playlist_id}")

    def adicionar_item_playlist(
        self, playlist_id: int, hash_arquivo: str
    ) -> dict[str, Any] | None:
        """POST /playlists/{id}/items; a ordem é atribuída pelo tracker."""
        return self._post(f"/playlists/{playlist_id}/items", {"hash": hash_arquivo})

    def remover_item_playlist(
        self, playlist_id: int, hash_arquivo: str
    ) -> dict[str, Any] | None:
        """DELETE /playlists/{id}/items/{hash}."""
        return self._delete(f"/playlists/{playlist_id}/items/{hash_arquivo}")

    def deletar_playlist(self, playlist_id: int) -> dict[str, Any] | None:
        """DELETE /playlists/{id}."""
        return self._delete(f"/playlists/{playlist_id}")

    # ------------------------------------------------------------------
    # Transporte com fallback
    # ------------------------------------------------------------------

    def _post(self, rota: str, corpo: dict[str, Any]) -> dict[str, Any] | None:
        """POST na API do tracker atual (ver _request)."""
        return self._request("POST", rota, corpo)

    def _get(self, rota: str) -> dict[str, Any] | None:
        """GET na API do tracker atual (ver _request)."""
        return self._request("GET", rota, None)

    def _delete(self, rota: str) -> dict[str, Any] | None:
        """DELETE na API do tracker atual (ver _request)."""
        return self._request("DELETE", rota, None)

    def _request(
        self, metodo: str, rota: str, corpo: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Requisita a API do tracker atual, com fallback aos demais em falha de rede.

        Returns:
            Corpo JSON da resposta; None em erro HTTP (não de conectividade).

        Raises:
            TodosTrackersIndisponiveis: Se todos os trackers da lista falharem
                por timeout/conexão recusada.
        """
        ultimo_erro: Exception | None = None
        for _ in range(len(self._trackers)):
            idx = self.current_tracker_index
            try:
                resposta = self._cliente(idx).request(metodo, rota, json=corpo)
                resposta.raise_for_status()
                dados = resposta.json()
                self._talvez_reassignar(dados)
                return dados
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                logger.warning(
                    "tracker %s indisponível na rota %s (%s); fallback para o próximo",
                    self._id(idx),
                    rota,
                    exc,
                )
                ultimo_erro = exc
                self._avancar_e_reapresentar(rota)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "tracker %s rota=%s HTTP %d: %s",
                    self._id(idx),
                    rota,
                    exc.response.status_code,
                    exc.response.text,
                )
                return None
            except httpx.HTTPError:
                logger.exception("erro HTTP no tracker %s rota=%s", self._id(idx), rota)
                return None
        raise TodosTrackersIndisponiveis(
            f"nenhum dos {len(self._trackers)} trackers respondeu à rota {rota}"
        ) from ultimo_erro

    def _avancar_e_reapresentar(self, rota: str) -> None:
        """Passa ao próximo tracker e reenvia PEER_HELLO lá."""
        self.current_tracker_index = (self.current_tracker_index + 1) % len(
            self._trackers
        )
        # A própria chamada de PEER_HELLO não deve recursar; sem identidade
        # (antes do primeiro hello) também não há o que reapresentar.
        if rota != "/peers/hello" and self._identidade is not None:
            self._hello_direto()

    def _talvez_reassignar(self, dados: object) -> None:
        """Migra para o tracker de reassign_to, se a resposta trouxer um."""
        if not isinstance(dados, dict) or self._reassignando:
            return
        alvo = dados.get("reassign_to")
        if not isinstance(alvo, dict):
            return
        self._reassignando = True
        try:
            self._migrar_para(str(alvo["ip"]), int(alvo["api_port"]))
        except (KeyError, TypeError, ValueError):
            logger.warning("reassign_to malformado, ignorado: %r", alvo)
        finally:
            self._reassignando = False

    def _migrar_para(self, ip: str, api_port: int) -> None:
        """Aponta o cliente ao tracker (ip, api_port) e se reapresenta lá."""
        for idx, tracker in enumerate(self._trackers):
            if str(tracker["ip"]) == ip and int(tracker["api_port"]) == api_port:
                self.current_tracker_index = idx
                break
        else:
            self._trackers.append(
                {"tracker_id": f"{ip}:{api_port}", "ip": ip, "api_port": api_port}
            )
            self.current_tracker_index = len(self._trackers) - 1
        logger.info("REASSIGN_TRACKER: peer migrado para %s:%d", ip, api_port)
        if self._identidade is not None:
            self._hello_direto()

    def _hello_direto(self) -> None:
        """Envia um PEER_HELLO ao tracker atual, sem fallback nem reassign."""
        assert self._identidade is not None
        nome_peer, ip, porta = self._identidade
        corpo = PeerHello(
            nome_peer=nome_peer, ip=ip, porta=porta, migrando=True
        ).model_dump()
        try:
            resposta = self._cliente(self.current_tracker_index).post(
                "/peers/hello", json=corpo
            )
            resposta.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "PEER_HELLO ao novo tracker %s falhou (%s)", self.tracker_id, exc
            )

    # ------------------------------------------------------------------
    # Sessões HTTP por tracker
    # ------------------------------------------------------------------

    def _base_url(self, idx: int) -> str:
        tracker = self._trackers[idx]
        return f"http://{tracker['ip']}:{tracker['api_port']}"

    def _id(self, idx: int) -> str:
        return str(self._trackers[idx].get("tracker_id", self._base_url(idx)))

    def _cliente(self, idx: int) -> httpx.Client:
        """Reusa (ou cria) a sessão HTTP do tracker idx."""
        cliente = self._clientes.get(idx)
        if cliente is None:
            cliente = httpx.Client(base_url=self._base_url(idx), timeout=self.timeout)
            self._clientes[idx] = cliente
        return cliente
