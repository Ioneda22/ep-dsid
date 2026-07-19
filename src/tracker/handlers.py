"""Lógica de negócio do tracker (camada 2 da arquitetura).

Cada handler recebe uma mensagem já validada (modelos pydantic de
src.common.messages) e as dependências por parâmetro —
Index e TrackerDB nunca são globais. Handlers não fazem I/O de
socket/HTTP: isso é papel da camada API (src.tracker.api).

Erros de domínio sobem como exceções de src.common.errors; a camada
API as converte em mensagens ERROR.
"""

from __future__ import annotations

import logging
from typing import Any

from src.common.messages import (
    PeerHello,
    PeerLeave,
    PeerLeaveFile,
    RegisterFile,
    SearchFile,
    SearchResult,
    SeedReport,
    SyncTableEntry,
    UpdateIp,
)
from src.tracker.index import Index, LocalDelta
from src.tracker.persistence import TrackerDB
from src.tracker.rebalance import RebalanceManager
from src.tracker.routing import SearchRouter
from src.tracker.sync_client import SyncClient

logger = logging.getLogger(__name__)

#: Resposta das operações de escrita: {"status": "ok"} e, quando o rebalance
#: agendou uma migração para este peer, também reassign_to (destino do REASSIGN_TRACKER).
AckOk = dict[str, Any]


def _ack(index: Index | None = None, nome_peer: str | None = None) -> AckOk:
    """Monta o ACK, anexando reassign_to se houver migração pendente ao peer."""
    resposta: AckOk = {"status": "ok"}
    if index is not None and nome_peer is not None:
        alvo = index.consumir_reassign(nome_peer)
        if alvo is not None:
            resposta["reassign_to"] = {"ip": alvo[0], "api_port": alvo[1]}
            logger.info(
                "REASSIGN_TRACKER: peer %s -> %s:%d", nome_peer, alvo[0], alvo[1]
            )
            index.remover_peer_local(nome_peer)
    return resposta


def _propagar(sync_client: SyncClient | None, delta: LocalDelta | None) -> None:
    """Floods as entradas de um evento multi-hash com o seq/timestamp únicos."""
    if sync_client is None or delta is None:
        return
    sync_client.propagar_sync(delta.entries, seq=delta.seq, timestamp=delta.timestamp)


def handle_peer_hello(
    msg: PeerHello,
    index: Index,
    db: TrackerDB,
    rebalance: RebalanceManager | None = None,
) -> AckOk:
    """Registra a presença do peer no índice e o usuário no SQLite.

    Com rebalance e sem uma migração já enfileirada, um sorteio inline pode
    devolver reassign_to para espalhar o peer entre os trackers — decisão única,
    não persistida, de modo que o peer migre no máximo uma vez.
    """
    index.register_peer(msg.nome_peer, msg.ip, msg.porta)
    db.registrar_usuario(msg.nome_peer)
    logger.info(
        "PEER_HELLO: nome_peer=%s endereco=%s:%d", msg.nome_peer, msg.ip, msg.porta
    )
    resposta = _ack(index, msg.nome_peer)
    if rebalance is not None and "reassign_to" not in resposta:
        alvo = rebalance.sortear_reassign()
        if alvo is not None:
            resposta["reassign_to"] = {"ip": alvo[0], "api_port": alvo[1]}
            logger.info(
                "REASSIGN_TRACKER (sorteio): peer %s -> %s:%d",
                msg.nome_peer,
                alvo[0],
                alvo[1],
            )
            index.remover_peer_local(msg.nome_peer)
    return resposta


def handle_peer_leave(
    msg: PeerLeave, index: Index, sync_client: SyncClient | None = None
) -> AckOk:
    """Saída ordenada: remove o peer, tombstona suas fontes e propaga o delta aos demais trackers."""
    delta = index.remove_peer(msg.nome_peer)
    logger.info("PEER_LEAVE: nome_peer=%s", msg.nome_peer)
    _propagar(sync_client, delta)
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
    return _ack(index, msg.nome_peer)


def handle_seed_report(
    msg: SeedReport, index: Index, sync_client: SyncClient | None = None
) -> AckOk:
    """Sinal de vida + anti-entropy do índice.

    Re-registra a presença (o relatório carrega ip/porta justamente para
    reconstruir o índice após restart do tracker), reconcilia os hashes (hash
    omitido equivale a PEER_LEAVE_FILE) e propaga o delta resultante via
    SYNC_TABLE — a detecção por seq/digest é backstop.
    """
    index.register_peer(msg.nome_peer, msg.ip, msg.porta)
    delta = index.apply_seed_hashes(msg.nome_peer, set(msg.hashes))
    logger.debug(
        "SEED_REPORT: nome_peer=%s n_hashes=%d", msg.nome_peer, len(msg.hashes)
    )
    _propagar(sync_client, delta)
    return _ack(index, msg.nome_peer)


def handle_register_file(
    msg: RegisterFile, index: Index, sync_client: SyncClient | None = None
) -> AckOk:
    """Registra upload original ou re-registro pós-download.

    Com sync_client, propaga a atualização via flooding SYNC_TABLE
    (fluxo de upload) SEM bloquear a resposta REST — cada destino
    recebe em thread daemon própria. O timestamp propagado é o MESMO
    gravado no índice local, para o LWW convergir entre réplicas.
    """
    entry, meta = index.register_file(
        nome_peer=msg.nome_peer,
        hash_arquivo=msg.hash,
        nome=msg.nome,
        tamanho=msg.tamanho,
        n_chunks=msg.n_chunks,
    )
    logger.info(
        "REGISTER_FILE: nome_peer=%s hash=%s nome=%s", msg.nome_peer, msg.hash, msg.nome
    )
    if sync_client is not None:
        sync_client.propagar_sync(
            [
                SyncTableEntry(
                    hash=msg.hash,
                    nome_peer=msg.nome_peer,
                    ip=entry.ip,
                    porta=entry.porta,
                    ativo=True,
                    nome=meta.nome,
                    tamanho=meta.tamanho,
                    n_chunks=meta.n_chunks,
                )
            ],
            seq=entry.seq,
            timestamp=entry.timestamp,
        )
    return _ack(index, msg.nome_peer)


def handle_peer_leave_file(
    msg: PeerLeaveFile, index: Index, sync_client: SyncClient | None = None
) -> AckOk:
    """Remove o peer como fonte de um hash (vira tombstone).

    Com sync_client, propaga o tombstone via SYNC_TABLE com
    ativo=False e o timestamp gravado localmente.
    """
    tombstone = index.remove_peer_from_hash(msg.hash, msg.nome_peer)
    logger.info("PEER_LEAVE_FILE: nome_peer=%s hash=%s", msg.nome_peer, msg.hash)
    if sync_client is not None:
        sync_client.propagar_sync(
            [
                SyncTableEntry(
                    hash=msg.hash,
                    nome_peer=msg.nome_peer,
                    ip=tombstone.ip,
                    porta=tombstone.porta,
                    ativo=False,
                )
            ],
            seq=tombstone.seq,
            timestamp=tombstone.timestamp,
        )
    return _ack(index, msg.nome_peer)


def handle_search_file(
    msg: SearchFile, index: Index, search_router: SearchRouter | None = None
) -> SearchResult:
    """Busca por nome exato; com search_router, roteia via SEARCH_FORWARD.

    Sem router (testes/uso isolado), a busca é apenas local.
    resultados=[] significa "nada encontrado".
    """
    if search_router is not None:
        return search_router.handle_search_file_with_forwarding(msg)
    resultados = index.search_by_name(msg.query)
    logger.info(
        "SEARCH_FILE: query_id=%s query=%r hits=%d",
        msg.query_id,
        msg.query,
        len(resultados),
    )
    return SearchResult(query_id=msg.query_id, resultados=resultados)
