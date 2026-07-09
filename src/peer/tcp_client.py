"""Cliente TCP do peer: solicita listas de chunks e baixa chunks.

As conexões peer↔peer são persistentes: a primeira requisição a um
(ip, porta) abre o socket e as seguintes o reutilizam — vários
CHUNK_REQUEST trafegam na mesma conexão durante um download. Cada
conexão tem seu MessageReader (buffer entre mensagens).

Thread-safe para o pool paralelo de download: há um lock POR DESTINO,
então dois workers que pegam a mesma fonte serializam na única conexão dela
(um socket TCP não faz request/response concorrente), enquanto fontes DISTINTAS
baixam em paralelo — que é onde está o ganho do download distribuído. O dict de
conexões tem lock próprio para as mutações de cache.
"""

from __future__ import annotations

import logging
import socket
import threading

from src.common.messages import ChunkListRequest, ChunkRequest
from src.common.protocol import MessageReader, ProtocolError, send_json_line

logger = logging.getLogger(__name__)


class PeerTCPClient:
    """Cliente TCP com cache de conexões persistentes por peer fonte (thread-safe)."""

    def __init__(self, timeout: float = 10.0) -> None:
        """Inicializa o cliente.

        Args:
            timeout: Timeout por requisição em segundos
                (chunk_request_timeout_seconds do YAML).
        """
        self.timeout = timeout
        self._conexoes: dict[tuple[str, int], MessageReader] = {}
        self._dict_lock = threading.Lock()
        self._locks_por_destino: dict[tuple[str, int], threading.Lock] = {}

    def close_all(self) -> None:
        """Fecha todas as conexões abertas com peers fonte."""
        with self._dict_lock:
            readers = list(self._conexoes.values())
            self._conexoes.clear()
        for reader in readers:
            try:
                reader.sock.close()
            except OSError:
                logger.exception("erro ao fechar conexão com peer")

    # ------------------------------------------------------------------
    # Requisições do protocolo peer↔peer
    # ------------------------------------------------------------------

    def request_chunk_list(
        self, ip: str, porta: int, hash_arquivo: str
    ) -> list[int] | None:
        """Pergunta a um peer quais chunks de hash_arquivo ele possui.

        Returns:
            Índices disponíveis na fonte, ou None em erro/timeout.
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
            Payload binário do chunk, ou None em erro/timeout/ERROR.
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
        """Envia mensagem na conexão (reusada) e lê uma resposta.

        O lock por destino garante que só uma requisição está em voo por
        conexão — o request/response ficaria corrompido se dois workers do pool
        intercalassem envios no mesmo socket.
        """
        with self._lock_destino(ip, porta):
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

    def _lock_destino(self, ip: str, porta: int) -> threading.Lock:
        """Lock dedicado à conexão com (ip, porta) (criado sob demanda)."""
        chave = (ip, porta)
        with self._dict_lock:
            lock = self._locks_por_destino.get(chave)
            if lock is None:
                lock = threading.Lock()
                self._locks_por_destino[chave] = lock
            return lock

    def _obter_conexao(self, ip: str, porta: int) -> MessageReader:
        """Reusa a conexão com (ip, porta) ou abre uma nova.

        Chamado já sob o lock do destino, então não há corrida de abertura
        para a mesma fonte; o dict só precisa de _dict_lock para as leituras/
        escritas entre destinos diferentes.
        """
        chave = (ip, porta)
        with self._dict_lock:
            reader = self._conexoes.get(chave)
        if reader is not None:
            return reader
        sock = socket.create_connection((ip, porta), timeout=self.timeout)
        reader = MessageReader(sock)
        with self._dict_lock:
            self._conexoes[chave] = reader
        logger.debug("conexão aberta com fonte %s:%d", ip, porta)
        return reader

    def _descartar_conexao(self, ip: str, porta: int) -> None:
        """Fecha e esquece uma conexão que falhou."""
        with self._dict_lock:
            reader = self._conexoes.pop((ip, porta), None)
        if reader is None:
            return
        try:
            reader.sock.close()
        except OSError:
            logger.exception("erro ao fechar conexão com %s:%d", ip, porta)
