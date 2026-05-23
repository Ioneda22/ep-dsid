"""Helpers de I/O TCP do PeerSpot.

Convenções de framing (cf. §4.8 e §4.9 do ``CLAUDE.md``):

* Toda mensagem JSON é serializada em UTF-8 e terminada por ``\\n``.
* O receptor lê bytes até encontrar ``\\n``, decodifica como UTF-8 e
  faz ``json.loads`` no resultado.
* Transferência de chunk = cabeçalho JSON (com ``\\n``) seguido
  *imediatamente* do payload binário de exatamente ``payload_bytes`` bytes,
  na mesma conexão TCP.

Recepção sempre via :class:`MessageReader`. O reader mantém um buffer
interno entre leituras consecutivas, evitando perda de bytes quando o TCP
coalesce mensagens em um único ``recv`` — fenômeno comum em loopback e
realidade obrigatória no peer↔peer, onde várias mensagens trafegam na
mesma conexão (§7.1/§7.4 do ``CLAUDE.md``). Mesmo em conexões one-shot
(e.g. flooding ``SYNC_TABLE`` tracker→tracker), o reader é a API: o custo
de uma instância extra é desprezível e a uniformidade elimina a categoria
de bug "esqueci de usar o reader".

``MessageReader`` **não é thread-safe**: cada socket deve ser lido por uma
única thread, ou o acesso ao reader sincronizado externamente. No PeerSpot
isso é satisfeito naturalmente — servers TCP usam uma thread filha por
conexão aceita, e o downloader serializa requests/respostas com um lock
externo se reaproveitar conexão entre threads do pool.
"""

from __future__ import annotations

import json
import socket
from typing import Any

#: Tamanho do buffer interno usado ao ler do socket.
_RECV_CHUNK = 4096


class ProtocolError(Exception):
    """Erro de framing/protocolo na camada de transporte."""


class ConnectionClosedError(ProtocolError):
    """Conexão TCP fechada pelo outro lado antes do esperado."""


# ---------------------------------------------------------------------------
# Envio
# ---------------------------------------------------------------------------


def send_json_line(sock: socket.socket, msg: dict[str, Any]) -> None:
    """Serializa ``msg`` como JSON UTF-8 e envia terminado por ``\\n``.

    Args:
        sock: Socket TCP conectado.
        msg: Mensagem a enviar.

    Raises:
        OSError: Em falha de I/O no socket.
    """
    payload = json.dumps(msg, ensure_ascii=False).encode("utf-8") + b"\n"
    sock.sendall(payload)


def send_chunk(
    sock: socket.socket, header: dict[str, Any], payload: bytes
) -> None:
    """Envia um cabeçalho JSON (com ``\\n``) seguido do payload binário.

    O campo ``payload_bytes`` em ``header`` deve refletir ``len(payload)``;
    a função valida isso para evitar dessincronia entre as duas pontas.

    Args:
        sock: Socket TCP conectado.
        header: Cabeçalho JSON. Deve conter ``payload_bytes`` igual a
            ``len(payload)``.
        payload: Bytes a serem enviados logo após o cabeçalho.

    Raises:
        ValueError: Se ``header['payload_bytes']`` não bater com ``len(payload)``.
        OSError: Em falha de I/O.
    """
    declared = header.get("payload_bytes")
    if declared != len(payload):
        raise ValueError(
            f"payload_bytes={declared!r} não bate com len(payload)={len(payload)}"
        )
    send_json_line(sock, header)
    sock.sendall(payload)


# ---------------------------------------------------------------------------
# Leitura: helpers internos e função genérica recv_exact
# ---------------------------------------------------------------------------


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Lê exatamente ``n`` bytes do socket; bloqueia até completar.

    Não usa o buffer interno do :class:`MessageReader`; útil para casos onde
    não há buffer (ex.: testes, leituras isoladas). Em conexões persistentes,
    prefira :meth:`MessageReader.recv_chunk`.

    Args:
        sock: Socket TCP conectado.
        n: Número de bytes a ler (>= 0).

    Returns:
        Bytes lidos, com tamanho ``n``.

    Raises:
        ConnectionClosedError: Se a conexão fechar antes de receber ``n`` bytes.
        ValueError: Se ``n`` < 0.
    """
    if n < 0:
        raise ValueError(f"n deve ser >= 0; recebido {n}")
    if n == 0:
        return b""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(min(_RECV_CHUNK, n - len(buf)))
        if not chunk:
            raise ConnectionClosedError(
                f"Conexão fechada após {len(buf)}/{n} bytes"
            )
        buf.extend(chunk)
    return bytes(buf)


def _read_line(
    sock: socket.socket, prebuf: bytearray
) -> tuple[bytes, bytearray]:
    """Lê do socket até ``\\n``, reusando ``prebuf`` se já contiver bytes.

    Args:
        sock: Socket TCP conectado.
        prebuf: Buffer com bytes já lidos do socket em chamadas anteriores.
            Pode estar vazio.

    Returns:
        Tupla ``(linha_sem_newline, leftover)``: a linha como bytes (sem o
        ``\\n``) e o restante do buffer que veio depois do ``\\n``.

    Raises:
        ConnectionClosedError: Se a conexão fechar antes do delimitador.
    """
    buf = prebuf
    while True:
        idx = buf.find(b"\n")
        if idx != -1:
            line = bytes(buf[:idx])
            leftover = bytearray(buf[idx + 1 :])
            return line, leftover
        chunk = sock.recv(_RECV_CHUNK)
        if not chunk:
            raise ConnectionClosedError(
                "Conexão fechada antes do delimitador '\\n'"
            )
        buf.extend(chunk)


def _decode_json_line(line: bytes) -> dict[str, Any]:
    """Decodifica uma linha como JSON UTF-8 ou levanta :class:`ProtocolError`."""
    try:
        return json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"JSON inválido: {exc}") from exc


# ---------------------------------------------------------------------------
# MessageReader: única API de leitura de mensagens do PeerSpot
# ---------------------------------------------------------------------------


class MessageReader:
    """Leitor stateful que preserva bytes excedentes entre leituras.

    Esta é a **única** API de recepção de mensagens do PeerSpot — instancie
    uma vez por socket aceito/conectado e use o mesmo reader para todas as
    leituras subsequentes na conexão. O buffer interno garante que nenhum
    byte seja perdido entre chamadas, mesmo quando o TCP coalesce o
    delimitador ``\\n`` ou várias mensagens em um único ``recv``.

    Não é thread-safe: cada socket deve ser lido por uma única thread, ou
    o acesso ao reader sincronizado externamente. Ver docstring do módulo.

    Attributes:
        sock: O socket TCP de origem.
    """

    __slots__ = ("sock", "_buf")

    def __init__(self, sock: socket.socket) -> None:
        """Cria um reader ligado a ``sock``."""
        self.sock = sock
        self._buf: bytearray = bytearray()

    def settimeout(self, timeout: float | None) -> None:
        """Ajusta o timeout do socket subjacente."""
        self.sock.settimeout(timeout)

    def recv_json_line(self, timeout: float | None = None) -> dict[str, Any]:
        """Lê uma mensagem JSON terminada em ``\\n``.

        Bytes que cheguem após o ``\\n`` são preservados no buffer interno
        e usados na próxima chamada — pode ser o início do próximo registro
        (outra ``recv_json_line``) ou de um payload binário (``recv_chunk``).

        Args:
            timeout: Timeout em segundos; ``None`` mantém o atual.

        Returns:
            Dicionário decodificado.

        Raises:
            ConnectionClosedError: Se a conexão fechar antes do delimitador.
            ProtocolError: Se o JSON for inválido.
            socket.timeout: Se exceder o timeout configurado.
        """
        if timeout is not None:
            self.sock.settimeout(timeout)
        line, self._buf = _read_line(self.sock, self._buf)
        return _decode_json_line(line)

    def recv_chunk(self) -> tuple[dict[str, Any], bytes]:
        """Lê um cabeçalho JSON seguido do payload binário.

        Trata coalescimento TCP: bytes recebidos junto com o cabeçalho são
        usados como prefixo do payload; bytes que chegarem além de
        ``payload_bytes`` ficam no buffer para a próxima leitura (geralmente
        o cabeçalho da próxima resposta na mesma conexão).

        Returns:
            Tupla ``(header, payload)``.

        Raises:
            ProtocolError: Se o cabeçalho for inválido.
            ConnectionClosedError: Se a conexão fechar prematuramente.
        """
        line, self._buf = _read_line(self.sock, self._buf)
        header = _decode_json_line(line)
        pb = header.get("payload_bytes")
        if not isinstance(pb, int) or pb < 0:
            raise ProtocolError(
                f"Cabeçalho sem 'payload_bytes' inteiro >= 0: {header!r}"
            )
        if len(self._buf) >= pb:
            payload = bytes(self._buf[:pb])
            self._buf = bytearray(self._buf[pb:])
            return header, payload
        falta = pb - len(self._buf)
        payload = bytes(self._buf) + recv_exact(self.sock, falta)
        self._buf = bytearray()
        return header, payload


