"""Servidor TCP do peer: fornece chunks a outros peers.

Atende CHUNK_LIST_REQUEST → CHUNK_LIST e CHUNK_REQUEST →
CHUNK_DATA_HEADER + payload binário, com uma thread filha por
conexão aceita. As conexões peer↔peer são persistentes: o mesmo socket
serve múltiplas requisições em sequência, por isso cada conexão tem um
único MessageReader e o loop só termina quando o cliente fecha.
"""

from __future__ import annotations

import logging
import socket
import threading
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from src.common import errors
from src.common.errors import build_error_message
from src.common.messages import (
    ChunkDataHeader,
    ChunkList,
    ChunkListRequest,
    ChunkRequest,
)
from src.common.protocol import (
    ConnectionClosedError,
    MessageReader,
    ProtocolError,
    send_chunk,
    send_json_line,
)

if TYPE_CHECKING:
    from src.peer.storage import Storage

logger = logging.getLogger(__name__)


class PeerTCPServer:
    """Servidor TCP que serve chunks do Storage deste peer."""

    def __init__(self, ip: str, porta: int, storage: Storage) -> None:
        """Configura o servidor (não escuta até start).

        Args:
            ip: IP local a escutar (ex.: 127.0.0.1).
            porta: Porta a escutar; 0 escolhe porta livre (testes) —
                o valor real fica em self.porta após started.
            storage: Armazenamento local de onde os chunks são lidos.
        """
        self.ip = ip
        self.porta = porta
        self.storage = storage
        self.started = threading.Event()
        self._server_socket: socket.socket | None = None
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Sobe o servidor em uma thread daemon dedicada."""
        if self._running:
            logger.warning("servidor TCP já em execução em %s:%d", self.ip, self.porta)
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, name="peer-tcp-server", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Encerra o servidor; o accept pendente é interrompido pelo close."""
        self._running = False
        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                logger.exception("erro ao fechar socket do servidor")
        logger.info("servidor TCP parado")

    # ------------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind((self.ip, self.porta))
            self.porta = self._server_socket.getsockname()[1]
            self._server_socket.listen()
            self.started.set()
            logger.info("servidor TCP escutando em %s:%d", self.ip, self.porta)
            self._accept_loop()
        except OSError:
            if self._running:
                logger.exception(
                    "erro fatal no servidor TCP %s:%d", self.ip, self.porta
                )
        finally:
            self.started.set()  # nunca deixar quem espera bloqueado

    def _accept_loop(self) -> None:
        assert self._server_socket is not None
        while self._running:
            try:
                conn, addr = self._server_socket.accept()
            except OSError:
                if self._running:
                    logger.exception("erro no accept")
                return
            threading.Thread(
                target=self._handle_connection,
                args=(conn, addr),
                name=f"peer-conn-{addr[0]}:{addr[1]}",
                daemon=True,
            ).start()

    # ------------------------------------------------------------------
    # Conexões
    # ------------------------------------------------------------------

    def _handle_connection(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        """Atende uma conexão persistente até o cliente encerrá-la."""
        logger.debug("conexão aberta por %s:%d", *addr)
        reader = MessageReader(conn)
        try:
            with conn:
                while True:
                    header, _payload = reader.recv_message()
                    self._dispatch(conn, header, addr)
        except ConnectionClosedError:
            logger.debug("conexão encerrada por %s:%d", *addr)
        except ProtocolError:
            logger.exception("mensagem inválida de %s:%d", *addr)
        except OSError:
            logger.exception("erro de I/O com %s:%d", *addr)

    def _dispatch(
        self, conn: socket.socket, header: dict[str, Any], addr: tuple[str, int]
    ) -> None:
        tipo = header.get("type")
        match tipo:
            case "CHUNK_LIST_REQUEST":
                self._responder_chunk_list(conn, header)
            case "CHUNK_REQUEST":
                self._responder_chunk(conn, header)
            case _:
                logger.warning("tipo inesperado %r de %s:%d", tipo, *addr)
                send_json_line(
                    conn,
                    build_error_message(
                        str(tipo or ""),
                        "",
                        errors.MALFORMED_MESSAGE,
                        f"tipo não atendido pelo peer: {tipo!r}",
                    ),
                )

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _responder_chunk_list(
        self, conn: socket.socket, header: dict[str, Any]
    ) -> None:
        try:
            pedido = ChunkListRequest.model_validate(header)
        except ValidationError as exc:
            logger.warning("CHUNK_LIST_REQUEST malformado: %s", exc)
            send_json_line(
                conn,
                build_error_message(
                    "CHUNK_LIST_REQUEST", "", errors.MALFORMED_MESSAGE, str(exc)
                ),
            )
            return

        n_chunks = self.storage.get_chunk_count(pedido.hash)
        disponiveis = [
            i for i in range(n_chunks) if self.storage.has_chunk(pedido.hash, i)
        ]
        resposta = ChunkList(hash=pedido.hash, chunks_disponiveis=disponiveis)
        send_json_line(conn, resposta.model_dump())
        logger.debug(
            "CHUNK_LIST: hash=%s disponiveis=%d/%d",
            pedido.hash,
            len(disponiveis),
            n_chunks,
        )

    def _responder_chunk(self, conn: socket.socket, header: dict[str, Any]) -> None:
        try:
            pedido = ChunkRequest.model_validate(header)
        except ValidationError as exc:
            logger.warning("CHUNK_REQUEST malformado: %s", exc)
            send_json_line(
                conn,
                build_error_message(
                    "CHUNK_REQUEST", "", errors.MALFORMED_MESSAGE, str(exc)
                ),
            )
            return

        try:
            dados = self.storage.load_chunk(pedido.hash, pedido.chunk_index)
        except FileNotFoundError:
            logger.warning(
                "chunk indisponível: hash=%s index=%d", pedido.hash, pedido.chunk_index
            )
            send_json_line(
                conn,
                build_error_message(
                    "CHUNK_REQUEST",
                    str(pedido.chunk_index),
                    errors.NOT_FOUND,
                    f"chunk {pedido.chunk_index} de {pedido.hash} indisponível",
                ),
            )
            return

        cabecalho = ChunkDataHeader(
            hash=pedido.hash,
            chunk_index=pedido.chunk_index,
            payload_bytes=len(dados),
        )
        send_chunk(conn, cabecalho.model_dump(), dados)
        logger.debug(
            "CHUNK_DATA: hash=%s index=%d bytes=%d",
            pedido.hash,
            pedido.chunk_index,
            len(dados),
        )
