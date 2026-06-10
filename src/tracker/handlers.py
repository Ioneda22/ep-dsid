"""Lógica de negócio do tracker (§6.1, camada 2).

Cada handler recebe uma mensagem já validada (modelos pydantic de
``src.common.messages``) e as dependências por parâmetro (§14.4) —
``Index`` e ``TrackerDB`` nunca são globais. Handlers não fazem I/O de
socket/HTTP: isso é papel da camada API (`src.tracker.api`).

Erros de domínio sobem como exceções de ``src.common.errors``; a camada
API as converte em mensagens ``ERROR``.
"""

from __future__ import annotations

import logging

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
from src.tracker.index import Index
from src.tracker.persistence import TrackerDB

logger = logging.getLogger(__name__)

#: Resposta padrão das operações de escrita bem-sucedidas (camada REST).
AckOk = dict[str, str]


def _ack() -> AckOk:
    return {"status": "ok"}


def handle_peer_hello(msg: PeerHello, index: Index, db: TrackerDB) -> AckOk:
    """Registra a presença do peer no índice e o usuário no SQLite."""
    index.register_peer(msg.nome_peer, msg.ip, msg.porta)
    db.registrar_usuario(msg.nome_peer)
    logger.info(
        "PEER_HELLO: nome_peer=%s endereco=%s:%d", msg.nome_peer, msg.ip, msg.porta
    )
    return _ack()


def handle_peer_leave(msg: PeerLeave, index: Index) -> AckOk:
    """Saída ordenada: remove o peer e tombstona todas as suas fontes."""
    index.remove_peer(msg.nome_peer)
    logger.info("PEER_LEAVE: nome_peer=%s", msg.nome_peer)
    return _ack()


def handle_update_ip(msg: UpdateIp, index: Index) -> AckOk:
    """Atualiza o endereço do peer em todas as tabelas do índice."""
    index.update_peer_address(msg.nome_peer, msg.novo_ip, msg.porta)
    logger.info(
        "UPDATE_IP: nome_peer=%s novo_endereco=%s:%d",
        msg.nome_peer,
        msg.novo_ip,
        msg.porta,
    )
    return _ack()


def handle_seed_report(msg: SeedReport, index: Index) -> AckOk:
    """Sinal de vida + anti-entropy do índice (main.tex §7.2).

    Re-registra a presença (o relatório carrega ip/porta justamente para
    reconstruir o índice após restart do tracker) e reconcilia os hashes:
    hash omitido em relação ao estado atual equivale a PEER_LEAVE_FILE.
    """
    index.register_peer(msg.nome_peer, msg.ip, msg.porta)
    index.apply_seed_hashes(msg.nome_peer, set(msg.hashes))
    logger.debug(
        "SEED_REPORT: nome_peer=%s n_hashes=%d", msg.nome_peer, len(msg.hashes)
    )
    return _ack()


def handle_register_file(msg: RegisterFile, index: Index) -> AckOk:
    """Registra upload original ou re-registro pós-download."""
    index.register_file(
        nome_peer=msg.nome_peer,
        hash_arquivo=msg.hash,
        nome=msg.nome,
        tamanho=msg.tamanho,
        n_chunks=msg.n_chunks,
    )
    logger.info(
        "REGISTER_FILE: nome_peer=%s hash=%s nome=%s", msg.nome_peer, msg.hash, msg.nome
    )
    return _ack()


def handle_peer_leave_file(msg: PeerLeaveFile, index: Index) -> AckOk:
    """Remove o peer como fonte de um hash (vira tombstone)."""
    index.remove_peer_from_hash(msg.hash, msg.nome_peer)
    logger.info("PEER_LEAVE_FILE: nome_peer=%s hash=%s", msg.nome_peer, msg.hash)
    return _ack()


def handle_search_file(msg: SearchFile, index: Index) -> SearchResult:
    """Busca local por nome exato. SEM SEARCH_FORWARD nesta fase (Fase 4).

    ``resultados=[]`` significa "nada encontrado" (main.tex §7.2).
    """
    resultados = index.search_by_name(msg.query)
    logger.info(
        "SEARCH_FILE: query_id=%s query=%r hits=%d",
        msg.query_id,
        msg.query,
        len(resultados),
    )
    return SearchResult(query_id=msg.query_id, resultados=resultados)
