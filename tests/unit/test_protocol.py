"""Testes unitários para src/common/protocol.py.

Usa ``socket.socketpair`` para roundtrip sem rede real. No Windows o socketpair
da stdlib emula com AF_INET — funciona normalmente para nossos propósitos.

Toda recepção passa pelo método único :meth:`MessageReader.recv_message`
(cf. docstring de ``src/common/protocol.py``).
"""

from __future__ import annotations

import os
import socket
import threading

import pytest

from src.common.protocol import (
    ConnectionClosedError,
    MessageReader,
    ProtocolError,
    recv_exact,
    send_chunk,
    send_json_line,
)


def _socketpair() -> tuple[socket.socket, socket.socket]:
    """Wrapper portátil para socket.socketpair (Windows usa AF_INET emulado)."""
    return socket.socketpair()


# ---------------------------------------------------------------------------
# JSON-pura: cabeçalho sem payload_bytes → payload retornado é None
# ---------------------------------------------------------------------------


def test_roundtrip_json_pura_simples() -> None:
    a, b = _socketpair()
    try:
        msg = {
            "type": "PEER_HELLO",
            "nome_peer": "alice",
            "ip": "127.0.0.1",
            "porta": 7001,
        }
        send_json_line(a, msg)
        header, payload = MessageReader(b).recv_message()
        assert header == msg
        assert payload is None
    finally:
        a.close()
        b.close()


def test_roundtrip_request_response() -> None:
    """Cliente envia uma linha; servidor responde. Cada lado usa seu próprio reader."""
    a, b = _socketpair()
    try:
        req = {"type": "CHUNK_LIST_REQUEST", "hash": "h" * 64}
        send_json_line(a, req)
        h, p = MessageReader(b).recv_message()
        assert h == req
        assert p is None

        resp = {"type": "CHUNK_LIST", "hash": "h" * 64, "chunks_disponiveis": [0, 2, 4]}
        send_json_line(b, resp)
        h, p = MessageReader(a).recv_message()
        assert h == resp
        assert p is None
    finally:
        a.close()
        b.close()


def test_multiplas_mensagens_json_puras_consecutivas() -> None:
    """Várias mensagens JSON-puras em sequência — mesmo reader, mesmo socket."""
    a, b = _socketpair()
    try:
        msgs = [
            {"type": "PEER_LEAVE", "nome_peer": "alice"},
            {"type": "PEER_LEAVE", "nome_peer": "bob"},
            {"type": "PEER_LEAVE", "nome_peer": "carol"},
        ]
        for m in msgs:
            send_json_line(a, m)
        reader = MessageReader(b)
        for esperado in msgs:
            h, p = reader.recv_message()
            assert h == esperado
            assert p is None
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Chunk (header + payload): cabeçalho com payload_bytes → payload é bytes
# ---------------------------------------------------------------------------


def test_roundtrip_chunk_payload_pequeno() -> None:
    a, b = _socketpair()
    try:
        payload = b"hello world"
        header = {
            "type": "CHUNK_DATA",
            "hash": "h" * 64,
            "chunk_index": 7,
            "payload_bytes": len(payload),
        }
        send_chunk(a, header, payload)
        h, p = MessageReader(b).recv_message()
        assert h == header
        assert p == payload
    finally:
        a.close()
        b.close()


def test_roundtrip_chunk_payload_grande() -> None:
    """Payload > 256 KiB força múltiplos recvs no lado receptor."""
    a, b = _socketpair()
    try:
        payload = os.urandom(262144 + 13)
        header = {
            "type": "CHUNK_DATA",
            "hash": "a" * 64,
            "chunk_index": 0,
            "payload_bytes": len(payload),
        }

        def send_side() -> None:
            send_chunk(a, header, payload)

        t = threading.Thread(target=send_side)
        t.start()
        h, p = MessageReader(b).recv_message()
        t.join()

        assert h == header
        assert p == payload
        assert p is not None and len(p) == header["payload_bytes"]
    finally:
        a.close()
        b.close()


def test_chunk_payload_zero_bytes() -> None:
    """payload_bytes=0 retorna bytes vazios (não None), distinto da JSON-pura."""
    a, b = _socketpair()
    try:
        header = {
            "type": "CHUNK_DATA",
            "hash": "z" * 64,
            "chunk_index": 0,
            "payload_bytes": 0,
        }
        send_chunk(a, header, b"")
        h, p = MessageReader(b).recv_message()
        assert h == header
        assert p == b""
        assert p is not None  # Distinto de JSON-pura
    finally:
        a.close()
        b.close()


def test_chunk_coalescimento_tcp() -> None:
    """Header e payload chegando no mesmo recv: reader deve separar corretamente."""
    a, b = _socketpair()
    try:
        payload = b"binario-de-teste"
        header = {
            "type": "CHUNK_DATA",
            "hash": "c" * 64,
            "chunk_index": 1,
            "payload_bytes": len(payload),
        }
        send_chunk(a, header, payload)
        h, p = MessageReader(b).recv_message()
        assert h == header
        assert p == payload
    finally:
        a.close()
        b.close()


def test_send_chunk_payload_bytes_inconsistente() -> None:
    a, b = _socketpair()
    try:
        header = {
            "type": "CHUNK_DATA",
            "hash": "x" * 64,
            "chunk_index": 0,
            "payload_bytes": 10,
        }
        with pytest.raises(ValueError, match="payload_bytes"):
            send_chunk(a, header, b"abc")
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Caso central: múltiplas mensagens reusando a mesma conexão
# ---------------------------------------------------------------------------


def test_multiplos_chunks_na_mesma_conexao() -> None:
    """Vários CHUNK_DATA reusando a mesma conexão TCP (cenário real do downloader).

    Lado A pré-empilha 3 respostas antes do lado B começar a ler — força
    coalescimento agressivo. O buffer interno do reader garante que nenhum
    byte seja descartado entre leituras consecutivas.
    """
    a, b = _socketpair()
    try:
        payloads = [os.urandom(1024), os.urandom(2048), os.urandom(512)]
        for i, p in enumerate(payloads):
            header = {
                "type": "CHUNK_DATA",
                "hash": "d" * 64,
                "chunk_index": i,
                "payload_bytes": len(p),
            }
            send_chunk(a, header, p)

        reader = MessageReader(b)
        for i, esperado in enumerate(payloads):
            h, p = reader.recv_message()
            assert h["chunk_index"] == i
            assert h["payload_bytes"] == len(esperado)
            assert p == esperado
    finally:
        a.close()
        b.close()


def test_mistura_json_pura_e_chunk_mesmo_reader() -> None:
    """Fluxo real do downloader (§7.4 CLAUDE.md): CHUNK_LIST seguida de CHUNK_DATA.

    Mesma conexão, mesmo reader, **mesmo método**. Sem buffer compartilhado, o
    início do cabeçalho do CHUNK_DATA seria perdido ao ler o CHUNK_LIST. Sem
    o método unificado, o receptor precisaria escolher manualmente entre dois
    leitores — exatamente o foot-gun que ``recv_message`` elimina.
    """
    cliente, servidor = _socketpair()
    try:
        send_json_line(
            servidor,
            {"type": "CHUNK_LIST", "hash": "h" * 64, "chunks_disponiveis": [0, 1, 2]},
        )
        payload = os.urandom(3000)
        send_chunk(
            servidor,
            {
                "type": "CHUNK_DATA",
                "hash": "h" * 64,
                "chunk_index": 1,
                "payload_bytes": len(payload),
            },
            payload,
        )

        reader = MessageReader(cliente)
        primeiro_h, primeiro_p = reader.recv_message()
        assert primeiro_h["type"] == "CHUNK_LIST"
        assert primeiro_h["chunks_disponiveis"] == [0, 1, 2]
        assert primeiro_p is None

        h, p = reader.recv_message()
        assert h["chunk_index"] == 1
        assert p == payload
    finally:
        cliente.close()
        servidor.close()


def test_chunk_payload_zero_seguido_de_outra_mensagem() -> None:
    """Payload de 0 bytes não deve confundir o consumo da próxima mensagem."""
    a, b = _socketpair()
    try:
        header = {
            "type": "CHUNK_DATA",
            "hash": "0" * 64,
            "chunk_index": 0,
            "payload_bytes": 0,
        }
        send_chunk(a, header, b"")
        send_json_line(a, {"type": "PEER_LEAVE", "nome_peer": "x"})

        reader = MessageReader(b)
        h, p = reader.recv_message()
        assert h == header
        assert p == b""

        h2, p2 = reader.recv_message()
        assert h2 == {"type": "PEER_LEAVE", "nome_peer": "x"}
        assert p2 is None
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Modos de falha
# ---------------------------------------------------------------------------


def test_conexao_fechada_durante_leitura() -> None:
    a, b = _socketpair()
    a.close()
    try:
        with pytest.raises(ConnectionClosedError):
            MessageReader(b).recv_message()
    finally:
        b.close()


def test_json_invalido_no_cabecalho() -> None:
    a, b = _socketpair()
    try:
        a.sendall(b"isto nao eh json\n")
        with pytest.raises(ProtocolError):
            MessageReader(b).recv_message()
    finally:
        a.close()
        b.close()


def test_payload_bytes_invalido_no_cabecalho() -> None:
    """payload_bytes presente mas com valor inválido (não int ou negativo)."""
    a, b = _socketpair()
    try:
        a.sendall(b'{"type": "CHUNK_DATA", "payload_bytes": -1}\n')
        with pytest.raises(ProtocolError, match="payload_bytes"):
            MessageReader(b).recv_message()
    finally:
        a.close()
        b.close()


def test_payload_bytes_nao_inteiro() -> None:
    a, b = _socketpair()
    try:
        a.sendall(b'{"type": "CHUNK_DATA", "payload_bytes": "abc"}\n')
        with pytest.raises(ProtocolError, match="payload_bytes"):
            MessageReader(b).recv_message()
    finally:
        a.close()
        b.close()


def test_recv_exact_lendo_zero_bytes() -> None:
    a, b = _socketpair()
    try:
        assert recv_exact(b, 0) == b""
    finally:
        a.close()
        b.close()


def test_recv_exact_conexao_fechada_antes_de_completar() -> None:
    a, b = _socketpair()
    try:
        a.sendall(b"ab")
        a.close()
        with pytest.raises(ConnectionClosedError):
            recv_exact(b, 10)
    finally:
        b.close()


def test_recv_exact_n_negativo() -> None:
    a, b = _socketpair()
    try:
        with pytest.raises(ValueError):
            recv_exact(b, -1)
    finally:
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Travas arquiteturais: APIs antigas não devem mais existir
# ---------------------------------------------------------------------------


def test_funcoes_de_modulo_removidas() -> None:
    """Funções one-shot de módulo foram removidas (Saída 2).

    Toda leitura passa por MessageReader.recv_message.
    """
    from src.common import protocol

    assert not hasattr(protocol, "recv_json_line"), (
        "recv_json_line de módulo foi removida intencionalmente (Saída 2); "
        "use MessageReader(sock).recv_message()"
    )
    assert not hasattr(protocol, "recv_chunk"), (
        "recv_chunk de módulo foi removida intencionalmente (Saída 2); "
        "use MessageReader(sock).recv_message()"
    )


def test_metodos_separados_do_reader_removidos() -> None:
    """recv_json_line/recv_chunk como métodos de MessageReader também foram removidos.

    Manter dois métodos forçava o caller a adivinhar o tipo enviado pelo emissor
    — mesma classe de foot-gun das funções one-shot. recv_message decide pelo
    próprio cabeçalho. Esta trava impede reintrodução acidental.
    """
    assert not hasattr(MessageReader, "recv_json_line"), (
        "MessageReader.recv_json_line foi removido; use recv_message() — "
        "o próprio cabeçalho decide se há payload"
    )
    assert not hasattr(MessageReader, "recv_chunk"), (
        "MessageReader.recv_chunk foi removido; use recv_message() — "
        "o próprio cabeçalho decide se há payload"
    )
