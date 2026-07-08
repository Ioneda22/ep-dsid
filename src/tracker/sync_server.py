"""Servidor TCP de sincronização entre trackers (§6.1, camada 4).

Espelha o ``servidor_sync``/``tratar_conexao`` do Listing 8.1 do
``main.tex``: ``socket`` + ``threading`` — NUNCA asyncio (§11.2). Escuta na
``sync_port`` dedicada (default 9001), SEPARADO do servidor FastAPI da
``api_port``; ambos vivem no mesmo processo Python, em threads distintas.

Mensagens atendidas:

* ``SYNC_TABLE`` — avança o vetor de versões, detecta lacuna de ``seq`` e
  aplica cada entry via ``Index.apply_sync_entry`` (LWW). Havendo lacuna,
  dispara ``SYNC_PULL`` para a origem (detecção inline, main.tex §11.3);
* ``SYNC_PULL`` — devolve, na MESMA conexão, uma ou mais ``SYNC_TABLE`` com o
  que a origem pedida originou acima de ``desde_seq`` (reparo direcionado);
* ``SYNC_DIGEST`` — compara o vetor de versões recebido com o local e puxa via
  ``SYNC_PULL`` o que o emissor tiver a mais (backstop periódico);
* ``SEARCH_FORWARD`` — busca local e devolve ``SEARCH_RESULT`` na mesma
  conexão (ver ``src.tracker.routing``);
* ``TRACKER_REJOIN`` — o tracker que recebe (o primeiro conhecido que aceitou a
  conexão do que volta) responde ``TRACKER_LIST`` e propaga ``TRACKER_ANNOUNCE``;
  o índice o tracker que volta reconstrói via ``SYNC_PULL(desde_seq=0)``
  (main.tex §12.3);
* ``TRACKER_ANNOUNCE`` — acrescenta o novo tracker à membership local.
"""

from __future__ import annotations

import logging
import socket
import threading

from pydantic import ValidationError

from src.common.messages import (
    SearchForward,
    SyncDigest,
    SyncPull,
    SyncPullItem,
    SyncTable,
    TrackerAnnounce,
    TrackerAnnounceNew,
    TrackerList,
    TrackerListItem,
    TrackerRejoin,
)
from src.common.protocol import (
    ConnectionClosedError,
    MessageReader,
    ProtocolError,
    send_json_line,
)
from src.tracker.index import Index
from src.tracker.rebalance import RebalanceManager
from src.tracker.routing import handle_search_forward
from src.tracker.sync_client import KnownTracker, SyncClient

logger = logging.getLogger(__name__)


class SyncServer:
    """Servidor TCP de ``sync_port``: aceita conexões em loop, uma thread cada.

    ``sync_port=0`` deixa o SO escolher uma porta livre (testes); o valor
    efetivo fica em ``porta`` após ``start``. O ``sync_client`` (para disparar
    pulls e propagar anúncios) pode ser passado no construtor ou atribuído
    depois — a ordem de criação no cluster resolve as portas dinâmicas antes de
    montar os clientes.

    Exemplo:
        >>> server = SyncServer("tracker-1", "127.0.0.1", 9001, index)
        >>> server.start()
        >>> server.started.wait(timeout=5)
    """

    def __init__(
        self,
        tracker_id: str,
        ip: str,
        sync_port: int,
        index: Index,
        sync_client: SyncClient | None = None,
        rebalance: RebalanceManager | None = None,
    ) -> None:
        self.tracker_id = tracker_id
        self.ip = ip
        self.porta = sync_port
        self.index = index
        self.sync_client = sync_client
        self.rebalance = rebalance
        self.started = threading.Event()
        self._encerrando = False
        self._srv: socket.socket | None = None

    def start(self) -> None:
        """Faz bind/listen e dispara a thread de ``accept`` (Listing 8.1)."""
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.ip, self.porta))
        srv.listen()
        self._srv = srv
        self.porta = srv.getsockname()[1]
        threading.Thread(
            target=self._loop_accept,
            name=f"sync-server-{self.tracker_id}",
            daemon=True,
        ).start()
        self.started.set()
        logger.info(
            "tracker_id=%s servidor de sync escutando em %s:%d",
            self.tracker_id,
            self.ip,
            self.porta,
        )

    def stop(self) -> None:
        """Encerra o socket de escuta; conexões em curso terminam sozinhas."""
        self._encerrando = True
        if self._srv is not None:
            self._srv.close()

    # ------------------------------------------------------------------
    # Loop de accept e tratamento de conexão (Listing 8.1)
    # ------------------------------------------------------------------

    def _loop_accept(self) -> None:
        assert self._srv is not None
        while True:
            try:
                conn, addr = self._srv.accept()
            except OSError:
                if self._encerrando:
                    return
                logger.exception(
                    "tracker_id=%s erro no accept do sync server", self.tracker_id
                )
                return
            threading.Thread(
                target=self._tratar_conexao,
                args=(conn, addr),
                name=f"sync-conn-{addr[0]}:{addr[1]}",
                daemon=True,
            ).start()

    def _tratar_conexao(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        """Lê mensagens da conexão até o remetente fechar e as despacha.

        O loop suporta o one-shot do flooding (uma ``SYNC_TABLE``/``SYNC_DIGEST``
        e fecha), o request/response do ``SEARCH_FORWARD`` e o ``SYNC_PULL``
        (após responder, ESTE lado fecha para sinalizar o fim das respostas).
        """
        with conn:
            reader = MessageReader(conn)
            while True:
                try:
                    header, _ = reader.recv_message()
                except ConnectionClosedError:
                    return  # remetente terminou — fim normal da conexão
                except ProtocolError:
                    logger.exception(
                        "tracker_id=%s mensagem malformada de %s:%d",
                        self.tracker_id,
                        addr[0],
                        addr[1],
                    )
                    return
                except OSError:
                    logger.exception(
                        "tracker_id=%s erro de I/O com %s:%d",
                        self.tracker_id,
                        addr[0],
                        addr[1],
                    )
                    return
                if not self._despachar(header, conn, addr):
                    return  # ex.: SYNC_PULL — fechamos após enviar as respostas

    def _despachar(
        self, header: dict[str, object], conn: socket.socket, addr: tuple[str, int]
    ) -> bool:
        """Despacha uma mensagem. Retorna ``False`` para encerrar a conexão."""
        tipo = header.get("type")
        try:
            if tipo == "SYNC_TABLE":
                self._tratar_sync_table(header)
            elif tipo == "SYNC_PULL":
                self._tratar_sync_pull(header, conn)
                return False  # respostas enviadas; fechar sinaliza o fim
            elif tipo == "SYNC_DIGEST":
                self._tratar_sync_digest(header)
            elif tipo == "SEARCH_FORWARD":
                self._tratar_search_forward(header, conn)
            elif tipo == "TRACKER_REJOIN":
                self._tratar_tracker_rejoin(header, conn)
            elif tipo == "TRACKER_ANNOUNCE":
                self._tratar_tracker_announce(header)
            else:
                logger.warning(
                    "tracker_id=%s tipo inesperado no sync port: %r de %s:%d",
                    self.tracker_id,
                    tipo,
                    addr[0],
                    addr[1],
                )
        except ValidationError:
            logger.exception(
                "tracker_id=%s payload inválido para %s de %s:%d",
                self.tracker_id,
                tipo,
                addr[0],
                addr[1],
            )
        return True

    # ------------------------------------------------------------------
    # Reconciliação: SYNC_TABLE / SYNC_PULL / SYNC_DIGEST
    # ------------------------------------------------------------------

    def _tratar_sync_table(self, header: dict[str, object]) -> None:
        msg = SyncTable.model_validate(header)
        if msg.origem == self.tracker_id:
            # Eco do próprio envio: o tracker_id em 'origem' existe exatamente
            # para evitar reprocessamento — main.tex §10 (Nomeação).
            logger.debug("tracker_id=%s ignorou eco de SYNC_TABLE", self.tracker_id)
            return
        # Detecta lacuna e avança o vetor de versões ANTES de aplicar: escritas
        # fora de ordem são aplicadas na hora; o desde_seq da pendência é o
        # visto capturado agora (main.tex §11.3).
        desde = self.index.registrar_recepcao_flood(msg.origem, msg.seq)
        aplicadas = sum(
            self.index.apply_sync_entry(entry, msg.origem, msg.timestamp, msg.seq)
            for entry in msg.entries
        )
        if desde is not None and self.sync_client is not None:
            self.sync_client.solicitar_pull_de(
                msg.origem, [SyncPullItem(origem=msg.origem, desde_seq=desde)]
            )
            logger.info(
                "SYNC_TABLE lacuna: tracker_id=%s origem=%s seq=%d desde=%d -> SYNC_PULL",
                self.tracker_id,
                msg.origem,
                msg.seq,
                desde,
            )
        logger.info(
            "SYNC_TABLE: tracker_id=%s origem=%s seq=%d timestamp=%.3f entries=%d aplicadas=%d",
            self.tracker_id,
            msg.origem,
            msg.seq,
            msg.timestamp,
            len(msg.entries),
            aplicadas,
        )

    def _tratar_sync_pull(self, header: dict[str, object], conn: socket.socket) -> None:
        msg = SyncPull.model_validate(header)
        respostas = self.index.selecionar_para_pull(msg.faltando)
        try:
            for tabela in respostas:
                send_json_line(conn, tabela.model_dump())
        except OSError:
            logger.warning(
                "tracker_id=%s não conseguiu responder SYNC_PULL", self.tracker_id
            )
            return
        logger.info(
            "SYNC_PULL atendido: tracker_id=%s pedido=%d respostas=%d",
            self.tracker_id,
            len(msg.faltando),
            len(respostas),
        )

    def _tratar_sync_digest(self, header: dict[str, object]) -> None:
        msg = SyncDigest.model_validate(header)
        if msg.origem == self.tracker_id:
            return
        faltando = self.index.comparar_digest(msg.versoes)
        if faltando and self.sync_client is not None:
            self.sync_client.solicitar_pull_de(msg.origem, faltando)
        logger.debug(
            "SYNC_DIGEST: tracker_id=%s origem=%s faltando=%d",
            self.tracker_id,
            msg.origem,
            len(faltando),
        )

    # ------------------------------------------------------------------
    # Busca roteada e membership de trackers
    # ------------------------------------------------------------------

    def _tratar_search_forward(
        self, header: dict[str, object], conn: socket.socket
    ) -> None:
        msg = SearchForward.model_validate(header)
        resultado = handle_search_forward(msg, self.index)
        try:
            send_json_line(conn, resultado.model_dump())
        except OSError:
            # Origem desistiu (timeout de 2s) antes da resposta — descartável.
            logger.warning(
                "tracker_id=%s não conseguiu devolver SEARCH_RESULT (query_id=%s)",
                self.tracker_id,
                msg.query_id,
            )

    def _tratar_tracker_rejoin(
        self, header: dict[str, object], conn: socket.socket
    ) -> None:
        """Responde ``TRACKER_LIST`` e propaga ``TRACKER_ANNOUNCE`` (main.tex §12.3).

        Qualquer tracker que aceite a conexão atua como ponto de entrada do que
        volta (o bootstrap é o primeiro conhecido reachable, não um nó fixo).
        """
        msg = TrackerRejoin.model_validate(header)
        if self.sync_client is not None:
            self.sync_client.adicionar_tracker(
                KnownTracker(msg.tracker_id, msg.ip, msg.porta)
            )
        lista = self._trackers_ativos()
        try:
            send_json_line(conn, TrackerList(trackers_conhecidos=lista).model_dump())
        except OSError:
            logger.warning(
                "tracker_id=%s não conseguiu responder TRACKER_LIST a %s",
                self.tracker_id,
                msg.tracker_id,
            )
            return
        self._anunciar_novo_tracker(msg)
        self._ceder_peers(msg.tracker_id)
        logger.info(
            "TRACKER_REJOIN de %s: respondi TRACKER_LIST (%d trackers) e anunciei",
            msg.tracker_id,
            len(lista),
        )

    def _tratar_tracker_announce(self, header: dict[str, object]) -> None:
        msg = TrackerAnnounce.model_validate(header)
        novo = msg.novo_tracker
        if novo.tracker_id == self.tracker_id or self.sync_client is None:
            return
        self.sync_client.adicionar_tracker(
            KnownTracker(novo.tracker_id, novo.ip, novo.porta)
        )
        self._ceder_peers(novo.tracker_id)
        logger.info("TRACKER_ANNOUNCE: tracker %s agora conhecido", novo.tracker_id)

    def _ceder_peers(self, novo_tracker_id: str) -> None:
        """Agenda a cessão de peers locais ao tracker reintegrado (rebalance, §6.5)."""
        if self.rebalance is not None:
            self.rebalance.ceder_peers_para(novo_tracker_id)

    def _trackers_ativos(self) -> list[TrackerListItem]:
        """Membership atual (este tracker + os conhecidos) para o ``TRACKER_LIST``."""
        itens = [
            TrackerListItem(tracker_id=self.tracker_id, ip=self.ip, porta=self.porta)
        ]
        if self.sync_client is not None:
            itens.extend(
                TrackerListItem(tracker_id=t.tracker_id, ip=t.ip, porta=t.sync_port)
                for t in self.sync_client.known_trackers
            )
        return itens

    def _anunciar_novo_tracker(self, rejoin: TrackerRejoin) -> None:
        """Propaga ``TRACKER_ANNOUNCE`` aos demais (menos ao que acabou de voltar)."""
        if self.sync_client is None:
            return
        anuncio = TrackerAnnounce(
            novo_tracker=TrackerAnnounceNew(
                tracker_id=rejoin.tracker_id, ip=rejoin.ip, porta=rejoin.porta
            )
        ).model_dump()
        for tracker in self.sync_client.known_trackers:
            if tracker.tracker_id == rejoin.tracker_id:
                continue
            threading.Thread(
                target=self.sync_client.enviar_unicast,
                args=(tracker, anuncio),
                name=f"announce-{tracker.tracker_id}",
                daemon=True,
            ).start()
