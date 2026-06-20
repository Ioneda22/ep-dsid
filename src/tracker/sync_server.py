"""Servidor TCP de sincronização entre trackers (§6.1, camada 4).

Espelha o ``servidor_sync``/``tratar_conexao`` do Listing 8.1 do
``main.tex``: ``socket`` + ``threading`` — NUNCA asyncio (§11.2). Escuta na
``sync_port`` dedicada (default 9001), SEPARADO do servidor FastAPI da
``api_port``; ambos vivem no mesmo processo Python, em threads distintas.

Mensagens atendidas nesta fase:

* ``SYNC_TABLE`` — aplica cada entry via ``Index.apply_sync_entry`` (LWW);
* ``FULL_SYNC`` — aplica o estado completo via ``Index.apply_full_sync``
  (LWW); é a reconciliação anti-entropy periódica entre trackers;
* ``SEARCH_FORWARD`` — busca local e devolve ``SEARCH_RESULT`` na mesma
  conexão (ver ``src.tracker.routing``);
* ``TRACKER_REJOIN`` / ``TRACKER_ANNOUNCE`` — reconhecidas e logadas, mas
  ignoradas até a Fase 5 (reintegração de trackers).
"""

from __future__ import annotations

import logging
import socket
import threading

from pydantic import ValidationError

from src.common.messages import FullSync, SearchForward, SyncTable
from src.common.protocol import (
    ConnectionClosedError,
    MessageReader,
    ProtocolError,
    send_json_line,
)
from src.tracker.index import Index
from src.tracker.routing import handle_search_forward

logger = logging.getLogger(__name__)

#: Tipos reconhecidos porém fora do escopo atual (reintegração — Fase 5).
_TIPOS_FASE_5 = ("TRACKER_REJOIN", "TRACKER_ANNOUNCE")


class SyncServer:
    """Servidor TCP de ``sync_port``: aceita conexões em loop, uma thread cada.

    ``sync_port=0`` deixa o SO escolher uma porta livre (testes); o valor
    efetivo fica em :attr:`porta` após :meth:`start`.

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
    ) -> None:
        self.tracker_id = tracker_id
        self.ip = ip
        self.porta = sync_port
        self.index = index
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

        O loop suporta tanto o one-shot do flooding (uma SYNC_TABLE e fecha)
        quanto o request/response do SEARCH_FORWARD na mesma conexão.
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
                self._despachar(header, conn, addr)

    def _despachar(
        self, header: dict[str, object], conn: socket.socket, addr: tuple[str, int]
    ) -> None:
        tipo = header.get("type")
        try:
            if tipo == "SYNC_TABLE":
                self._tratar_sync_table(header)
            elif tipo == "FULL_SYNC":
                self._tratar_full_sync(header)
            elif tipo == "SEARCH_FORWARD":
                self._tratar_search_forward(header, conn)
            elif tipo in _TIPOS_FASE_5:
                logger.info(
                    "tracker_id=%s recebeu %s de %s:%d — reintegração é Fase 5; ignorada",
                    self.tracker_id,
                    tipo,
                    addr[0],
                    addr[1],
                )
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

    def _tratar_sync_table(self, header: dict[str, object]) -> None:
        msg = SyncTable.model_validate(header)
        if msg.origem == self.tracker_id:
            # Eco do próprio envio: o tracker_id em 'origem' existe exatamente
            # para evitar reprocessamento — main.tex §10 (Nomeação).
            logger.debug("tracker_id=%s ignorou eco de SYNC_TABLE", self.tracker_id)
            return
        aplicadas = sum(
            self.index.apply_sync_entry(entry, msg.origem, msg.timestamp)
            for entry in msg.entries
        )
        logger.info(
            "SYNC_TABLE: tracker_id=%s origem=%s timestamp=%.3f entries=%d aplicadas=%d",
            self.tracker_id,
            msg.origem,
            msg.timestamp,
            len(msg.entries),
            aplicadas,
        )

    def _tratar_full_sync(self, header: dict[str, object]) -> None:
        msg = FullSync.model_validate(header)
        if msg.origem == self.tracker_id:
            # Defensivo: o push de anti-entropy nunca se inclui na lista de
            # destinos, mas ignorar o próprio eco é barato e seguro.
            logger.debug("tracker_id=%s ignorou eco de FULL_SYNC", self.tracker_id)
            return
        aplicadas = self.index.apply_full_sync(msg.entries)
        logger.info(
            "FULL_SYNC: tracker_id=%s origem=%s entries=%d aplicadas=%d",
            self.tracker_id,
            msg.origem,
            len(msg.entries),
            aplicadas,
        )

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
