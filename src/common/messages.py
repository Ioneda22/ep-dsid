"""Definições das mensagens do protocolo PeerSpot (Listing 7.2 do main.tex).

Todas as mensagens trocadas entre peers, entre tracker e peer, e entre trackers
são representadas como ``TypedDict`` para validação estática. Em runtime, as
mensagens trafegam como ``dict[str, Any]`` serializados em JSON.

Mantenha estas definições alinhadas literalmente com o Listing 7.2 do
``main.tex``. Qualquer divergência deve ser questionada antes de codar.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

# ---------------------------------------------------------------------------
# 1. Entrada do peer na rede e manutenção de presença
# ---------------------------------------------------------------------------


class PeerHello(TypedDict):
    """Apresentação inicial do peer ao tracker (peer -> tracker)."""

    type: Literal["PEER_HELLO"]
    nome_peer: str
    ip: str
    porta: int


class PeerLeave(TypedDict):
    """Peer saindo da rede de forma ordenada (peer -> tracker)."""

    type: Literal["PEER_LEAVE"]
    nome_peer: str


class UpdateIp(TypedDict):
    """Notificação de mudança de IP (peer -> tracker)."""

    type: Literal["UPDATE_IP"]
    nome_peer: str
    novo_ip: str
    porta: int


class SeedReport(TypedDict):
    """Relatório periódico de seed (peer -> tracker, a cada 3 minutos)."""

    type: Literal["SEED_REPORT"]
    nome_peer: str
    ip: str
    porta: int
    hashes: list[str]


# ---------------------------------------------------------------------------
# 2. Registro de arquivos e busca
# ---------------------------------------------------------------------------


class RegisterFile(TypedDict, total=False):
    """Registro de música (peer -> tracker).

    Os campos ``nome``, ``tamanho`` e ``n_chunks`` são opcionais ao re-registrar
    após download — o tracker já os conhece do upload original.
    """

    type: Literal["REGISTER_FILE"]
    nome_peer: str
    hash: str
    nome: str
    tamanho: int
    n_chunks: int


class SearchFile(TypedDict):
    """Busca de música (peer -> tracker)."""

    type: Literal["SEARCH_FILE"]
    query_id: str
    query: str
    ttl: int


class SearchForward(TypedDict):
    """Roteamento de busca entre trackers (tracker -> tracker)."""

    type: Literal["SEARCH_FORWARD"]
    query_id: str
    query: str
    ttl: int
    origem_tracker: str


class SearchResultPeer(TypedDict):
    """Entrada de peer dentro de um resultado de busca."""

    nome_peer: str
    ip: str
    porta: int


class SearchResultEntry(TypedDict):
    """Entrada de resultado de busca (um hash + seus peers)."""

    hash: str
    nome: str
    peers: list[SearchResultPeer]


class SearchResult(TypedDict):
    """Resposta com peers disponíveis (tracker -> peer ou tracker -> tracker)."""

    type: Literal["SEARCH_RESULT"]
    query_id: str
    resultados: list[SearchResultEntry]


# ---------------------------------------------------------------------------
# 3. Transferência peer-to-peer
# ---------------------------------------------------------------------------


class ChunkListRequest(TypedDict):
    """Solicitação da lista de chunks que um peer possui (peer -> peer)."""

    type: Literal["CHUNK_LIST_REQUEST"]
    hash: str


class ChunkList(TypedDict):
    """Resposta com a lista de chunks disponíveis (peer -> peer)."""

    type: Literal["CHUNK_LIST"]
    hash: str
    chunks_disponiveis: list[int]


class ChunkRequest(TypedDict):
    """Requisição de chunk (peer -> peer)."""

    type: Literal["CHUNK_REQUEST"]
    hash: str
    chunk_index: int


class ChunkDataHeader(TypedDict):
    """Cabeçalho que precede o payload binário de um chunk (peer -> peer)."""

    type: Literal["CHUNK_DATA"]
    hash: str
    chunk_index: int
    payload_bytes: int


# ---------------------------------------------------------------------------
# 4. Remoção de arquivos
# ---------------------------------------------------------------------------


class PeerLeaveFile(TypedDict):
    """Remoção explícita de um arquivo do índice (peer -> tracker)."""

    type: Literal["PEER_LEAVE_FILE"]
    nome_peer: str
    hash: str


# ---------------------------------------------------------------------------
# 5. Sincronização entre trackers (flooding TCP unicast)
# ---------------------------------------------------------------------------


class SyncTableEntry(TypedDict):
    """Entrada individual dentro de uma SYNC_TABLE."""

    hash: str
    nome_peer: str
    ip: str
    porta: int
    ativo: bool


class SyncTable(TypedDict):
    """Atualização incremental do índice (tracker -> tracker)."""

    type: Literal["SYNC_TABLE"]
    origem: str
    timestamp: float
    entries: list[SyncTableEntry]


class FullSyncPeer(TypedDict):
    """Peer dentro de uma entrada de FULL_SYNC (com flag ativo e timestamp)."""

    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    timestamp: float


class FullSyncEntry(TypedDict):
    """Entrada de FULL_SYNC: arquivo + peers conhecidos."""

    hash: str
    nome: str
    tamanho: int
    n_chunks: int
    peers: list[FullSyncPeer]


class FullSyncTracker(TypedDict):
    """Tracker dentro da lista ``trackers_conhecidos`` de um FULL_SYNC."""

    tracker_id: str
    ip: str
    porta: int


class FullSync(TypedDict):
    """Sincronização completa do índice (tracker -> tracker)."""

    type: Literal["FULL_SYNC"]
    origem: str
    entries: list[FullSyncEntry]
    trackers_conhecidos: list[FullSyncTracker]


# ---------------------------------------------------------------------------
# 6. Gestão de membros do grupo de trackers
# ---------------------------------------------------------------------------


class TrackerRejoin(TypedDict):
    """Novo tracker se apresentando ao bootstrap node (tracker -> tracker)."""

    type: Literal["TRACKER_REJOIN"]
    tracker_id: str
    ip: str
    porta: int


class TrackerAnnounceNew(TypedDict):
    """Descritor do novo tracker dentro de TRACKER_ANNOUNCE."""

    tracker_id: str
    ip: str
    porta: int


class TrackerAnnounce(TypedDict):
    """Anúncio de novo tracker aos demais (tracker -> tracker, flooding TCP)."""

    type: Literal["TRACKER_ANNOUNCE"]
    novo_tracker: TrackerAnnounceNew


class ReassignTracker(TypedDict):
    """Reatribuição de peer ao tracker reintegrado (tracker ativo -> peer)."""

    type: Literal["REASSIGN_TRACKER"]
    peer_nome: str
    novo_tracker_ip: str
    novo_tracker_porta: int


# ---------------------------------------------------------------------------
# 7. Sinalização de erro genérica
# ---------------------------------------------------------------------------


class ErrorMessage(TypedDict):
    """Resposta de erro a qualquer requisição (qualquer -> qualquer)."""

    type: Literal["ERROR"]
    ref_type: str
    ref_id: str
    code: str
    mensagem: str


# ---------------------------------------------------------------------------
# Tabela de tipos válidos e validação leve em runtime
# ---------------------------------------------------------------------------


REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "PEER_HELLO": ("nome_peer", "ip", "porta"),
    "PEER_LEAVE": ("nome_peer",),
    "UPDATE_IP": ("nome_peer", "novo_ip", "porta"),
    "SEED_REPORT": ("nome_peer", "ip", "porta", "hashes"),
    "REGISTER_FILE": ("nome_peer", "hash"),
    "SEARCH_FILE": ("query_id", "query", "ttl"),
    "SEARCH_FORWARD": ("query_id", "query", "ttl", "origem_tracker"),
    "SEARCH_RESULT": ("query_id", "resultados"),
    "CHUNK_LIST_REQUEST": ("hash",),
    "CHUNK_LIST": ("hash", "chunks_disponiveis"),
    "CHUNK_REQUEST": ("hash", "chunk_index"),
    "CHUNK_DATA": ("hash", "chunk_index", "payload_bytes"),
    "PEER_LEAVE_FILE": ("nome_peer", "hash"),
    "SYNC_TABLE": ("origem", "timestamp", "entries"),
    "FULL_SYNC": ("origem", "entries", "trackers_conhecidos"),
    "TRACKER_REJOIN": ("tracker_id", "ip", "porta"),
    "TRACKER_ANNOUNCE": ("novo_tracker",),
    "REASSIGN_TRACKER": ("peer_nome", "novo_tracker_ip", "novo_tracker_porta"),
    "ERROR": ("ref_type", "ref_id", "code", "mensagem"),
}
"""Mapa de ``type`` -> campos obrigatórios para validação leve em runtime."""


def validate_message(msg: dict[str, Any]) -> str:
    """Checa minimamente que ``msg`` tem ``type`` conhecido e campos obrigatórios.

    Args:
        msg: Dicionário recém-decodificado do JSON.

    Returns:
        O ``type`` da mensagem.

    Raises:
        ValueError: Se ``type`` estiver ausente, for desconhecido, ou se faltar
            algum campo obrigatório.
    """
    if not isinstance(msg, dict):
        raise ValueError(f"Mensagem deve ser dict; recebido {type(msg).__name__}")
    msg_type = msg.get("type")
    if not isinstance(msg_type, str):
        raise ValueError("Mensagem sem campo 'type' válido")
    if msg_type not in REQUIRED_FIELDS:
        raise ValueError(f"Tipo de mensagem desconhecido: {msg_type!r}")
    faltando = [f for f in REQUIRED_FIELDS[msg_type] if f not in msg]
    if faltando:
        raise ValueError(
            f"Mensagem {msg_type} sem campos obrigatórios: {faltando}"
        )
    return msg_type
