"""Cliente TCP do peer: solicita listas de chunks e baixa chunks (§7.4).

As conexões peer↔peer são persistentes: a primeira requisição a um
``(ip, porta)`` abre o socket e as seguintes o reutilizam — vários
``CHUNK_REQUEST`` trafegam na mesma conexão durante um download. Cada
conexão tem seu :class:`MessageReader` (buffer entre mensagens).

Não é thread-safe: na Fase 3 o download é sequencial em uma única
thread. O pool paralelo da Fase 5 deverá usar uma conexão por worker
ou sincronizar o acesso.
"""

from __future__ import annotations

import logging
import socket

from src.common.messages import ChunkListRequest, ChunkRequest
from src.common.protocol import MessageReader, ProtocolError, send_json_line

logger = logging.getLogger(__name__)


class PeerTCPClient:
    """Cliente TCP com cache de conexões persistentes por peer fonte."""

    def __init__(self, timeout: float = 10.0) -> None:
        """Inicializa o cliente.

        Args:
            timeout: Timeout por requisição em segundos
                (``chunk_request_timeout_seconds`` do YAML, §7.6).
        """
        self.timeout = timeout
        self._conexoes: dict[tuple[str, int], MessageReader] = {}

    def close_all(self) -> None:
        """Fecha todas as conexões abertas com peers fonte."""
        for reader in self._conexoes.values():
            try:
                reader.sock.close()
            except OSError:
                logger.exception("erro ao fechar conexão com peer")
        self._conexoes.clear()

    # ------------------------------------------------------------------
    # Requisições do protocolo peer↔peer
    # ------------------------------------------------------------------

    def request_chunk_list(
        self, ip: str, porta: int, hash_arquivo: str
    ) -> list[int] | None:
        """Pergunta a um peer quais chunks de ``hash_arquivo`` ele possui.

        Returns:
            Índices disponíveis na fonte, ou ``None`` em erro/timeout.
        """
        pedido = ChunkListRequest(hash=hash_arquivo)
        resposta = self._requisitar(ip, porta, pedido.model_dump())
        if resposta is None:
            return None
        header, _payload = resposta
        if header.get("type") != "CHUNK_LIST" or header.get("hash") != hash_arquivo:
            logger.warning("resposta inesperada a CHUNK_LIST_REQUEST: %r", header)
            return None
        return list(header.get("chunks_disponiveis", []))

    def download_chunk(
        self, ip: str, porta: int, hash_arquivo: str, chunk_index: int
    ) -> bytes | None:
        """Baixa um chunk de um peer fonte.

        Returns:
            Payload binário do chunk, ou ``None`` em erro/timeout/ERROR.
        """
        pedido = ChunkRequest(hash=hash_arquivo, chunk_index=chunk_index)
        resposta = self._requisitar(ip, porta, pedido.model_dump())
        if resposta is None:
            return None
        header, payload = resposta
        if header.get("type") == "ERROR":
            logger.warning(
                "fonte %s:%d respondeu ERROR ao chunk %d: %s",
                ip,
                porta,
                chunk_index,
                header.get("mensagem"),
            )
            return None
        if header.get("type") != "CHUNK_DATA" or payload is None:
            logger.warning("resposta inesperada a CHUNK_REQUEST: %r", header)
            return None
        if (
            header.get("chunk_index") != chunk_index
            or header.get("hash") != hash_arquivo
        ):
            logger.warning(
                "CHUNK_DATA fora de correlação: esperado %s[%d], veio %r",
                hash_arquivo,
                chunk_index,
                header,
            )
            return None
        return payload

    # ------------------------------------------------------------------
    # Transporte: conexão persistente + request/response
    # ------------------------------------------------------------------

    def _requisitar(
        self, ip: str, porta: int, mensagem: dict
    ) -> tuple[dict, bytes | None] | None:
        """Envia ``mensagem`` na conexão (reusada) e lê uma resposta."""
        try:
            reader = self._obter_conexao(ip, porta)
            send_json_line(reader.sock, mensagem)
            return reader.recv_message(timeout=self.timeout)
        except (TimeoutError, socket.timeout):
            logger.warning("timeout na fonte %s:%d", ip, porta)
            self._descartar_conexao(ip, porta)
            return None
        except (ProtocolError, OSError):
            logger.exception("falha na fonte %s:%d", ip, porta)
            self._descartar_conexao(ip, porta)
            return None

    def _obter_conexao(self, ip: str, porta: int) -> MessageReader:
        """Reusa a conexão com ``(ip, porta)`` ou abre uma nova."""
        chave = (ip, porta)
        reader = self._conexoes.get(chave)
        if reader is not None:
            return reader
        sock = socket.create_connection((ip, porta), timeout=self.timeout)
        reader = MessageReader(sock)
        self._conexoes[chave] = reader
        logger.debug("conexão aberta com fonte %s:%d", ip, porta)
        return reader

    def _descartar_conexao(self, ip: str, porta: int) -> None:
        """Fecha e esquece uma conexão que falhou."""
        reader = self._conexoes.pop((ip, porta), None)
        if reader is None:
            return
        try:
            reader.sock.close()
        except OSError:
            logger.exception("erro ao fechar conexão com %s:%d", ip, porta)
