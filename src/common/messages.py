"""Definições das mensagens do protocolo PeerSpot (Listing 7.2 do main.tex).

Todas as mensagens trocadas entre peers, entre tracker e peer, e entre
trackers são modelos **pydantic** (``BaseModel``): servem de validação de
schema em runtime (inclusive como corpos de requisição do FastAPI) e de
type hints estáticos. Na rede, trafegam como JSON (``model_dump()``).

Mantenha estas definições alinhadas literalmente com o Listing 7.2 do
``main.tex``. Qualquer divergência deve ser questionada antes de codar.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ValidationError

# ---------------------------------------------------------------------------
# 1. Entrada do peer na rede e manutenção de presença
# ---------------------------------------------------------------------------


class PeerHello(BaseModel):
    """Apresentação inicial do peer ao tracker (peer -> tracker)."""

    type: Literal["PEER_HELLO"] = "PEER_HELLO"
    nome_peer: str
    ip: str
    porta: int


class PeerLeave(BaseModel):
    """Peer saindo da rede de forma ordenada (peer -> tracker)."""

    type: Literal["PEER_LEAVE"] = "PEER_LEAVE"
    nome_peer: str


class UpdateIp(BaseModel):
    """Notificação de mudança de IP (peer -> tracker)."""

    type: Literal["UPDATE_IP"] = "UPDATE_IP"
    nome_peer: str
    novo_ip: str
    porta: int


class SeedReport(BaseModel):
    """Relatório periódico de seed (peer -> tracker, a cada 3 minutos)."""

    type: Literal["SEED_REPORT"] = "SEED_REPORT"
    nome_peer: str
    ip: str
    porta: int
    hashes: list[str]


# ---------------------------------------------------------------------------
# 2. Registro de arquivos e busca
# ---------------------------------------------------------------------------


class RegisterFile(BaseModel):
    """Registro de música (peer -> tracker).

    Os campos ``nome``, ``tamanho`` e ``n_chunks`` são opcionais ao
    re-registrar após download — o tracker já os conhece do upload
    original (main.tex §7.2).
    """

    type: Literal["REGISTER_FILE"] = "REGISTER_FILE"
    nome_peer: str
    hash: str
    nome: str | None = None
    tamanho: int | None = None
    n_chunks: int | None = None


class SearchFile(BaseModel):
    """Busca de música (peer -> tracker)."""

    type: Literal["SEARCH_FILE"] = "SEARCH_FILE"
    query_id: str
    query: str
    ttl: int  # valor inicial recomendado: 3 — main.tex §7.2


class SearchForward(BaseModel):
    """Roteamento de busca entre trackers (tracker -> tracker)."""

    type: Literal["SEARCH_FORWARD"] = "SEARCH_FORWARD"
    query_id: str
    query: str
    ttl: int
    origem_tracker: str


class SearchResultPeer(BaseModel):
    """Entrada de peer dentro de um resultado de busca."""

    nome_peer: str
    ip: str
    porta: int


class SearchResultEntry(BaseModel):
    """Entrada de resultado de busca (um hash + seus peers).

    ``n_chunks`` é uma extensão consciente do Listing 7.2 (autorizada):
    o peer precisa do total de chunks para montar o plano de download e
    o tracker já o conhece do ``REGISTER_FILE`` original — sem ele, o
    peer teria de inferi-lo dos ``CHUNK_LIST`` das fontes, o que falha
    quando nenhuma fonte tem o arquivo completo.
    """

    hash: str
    nome: str
    n_chunks: int
    peers: list[SearchResultPeer]


class SearchResult(BaseModel):
    """Resposta com peers disponíveis (tracker -> peer ou tracker -> tracker)."""

    type: Literal["SEARCH_RESULT"] = "SEARCH_RESULT"
    query_id: str
    resultados: list[SearchResultEntry]


# ---------------------------------------------------------------------------
# 3. Transferência peer-to-peer
# ---------------------------------------------------------------------------


class ChunkListRequest(BaseModel):
    """Solicitação da lista de chunks que um peer possui (peer -> peer)."""

    type: Literal["CHUNK_LIST_REQUEST"] = "CHUNK_LIST_REQUEST"
    hash: str


class ChunkList(BaseModel):
    """Resposta com a lista de chunks disponíveis (peer -> peer)."""

    type: Literal["CHUNK_LIST"] = "CHUNK_LIST"
    hash: str
    chunks_disponiveis: list[int]


class ChunkRequest(BaseModel):
    """Requisição de chunk (peer -> peer)."""

    type: Literal["CHUNK_REQUEST"] = "CHUNK_REQUEST"
    hash: str
    chunk_index: int


class ChunkDataHeader(BaseModel):
    """Cabeçalho que precede o payload binário de um chunk (peer -> peer)."""

    type: Literal["CHUNK_DATA"] = "CHUNK_DATA"
    hash: str
    chunk_index: int
    payload_bytes: int


# ---------------------------------------------------------------------------
# 4. Remoção de arquivos
# ---------------------------------------------------------------------------


class PeerLeaveFile(BaseModel):
    """Remoção explícita de um arquivo do índice (peer -> tracker)."""

    type: Literal["PEER_LEAVE_FILE"] = "PEER_LEAVE_FILE"
    nome_peer: str
    hash: str


# ---------------------------------------------------------------------------
# 5. Sincronização entre trackers (flooding TCP unicast)
# ---------------------------------------------------------------------------


class SyncTableEntry(BaseModel):
    """Entrada individual dentro de uma SYNC_TABLE.

    ``nome``/``tamanho``/``n_chunks`` são uma extensão consciente do
    Listing 7.2 (autorizada, refletida no main.tex): sem eles, um tracker
    que conhece o hash apenas via SYNC_TABLE não consegue responder buscas
    por nome nem aceitar o re-registro pós-download de um peer local —
    o FULL_SYNC já carrega esses metadados, a omissão no SYNC_TABLE era
    inconsistente. Opcionais: tombstones (``ativo=False``) não precisam
    deles.
    """

    hash: str
    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    nome: str | None = None
    tamanho: int | None = None
    n_chunks: int | None = None


class SyncTable(BaseModel):
    """Atualização incremental do índice (tracker -> tracker)."""

    type: Literal["SYNC_TABLE"] = "SYNC_TABLE"
    origem: str
    timestamp: float
    entries: list[SyncTableEntry]


class FullSyncPeer(BaseModel):
    """Peer dentro de uma entrada de FULL_SYNC (com flag ativo e timestamp)."""

    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    timestamp: float


class FullSyncEntry(BaseModel):
    """Entrada de FULL_SYNC: arquivo + peers conhecidos."""

    hash: str
    nome: str
    tamanho: int
    n_chunks: int
    peers: list[FullSyncPeer]


class FullSyncTracker(BaseModel):
    """Tracker dentro da lista ``trackers_conhecidos`` de um FULL_SYNC."""

    tracker_id: str
    ip: str
    porta: int


class FullSync(BaseModel):
    """Sincronização completa do índice (tracker -> tracker)."""

    type: Literal["FULL_SYNC"] = "FULL_SYNC"
    origem: str
    entries: list[FullSyncEntry]
    trackers_conhecidos: list[FullSyncTracker]


# ---------------------------------------------------------------------------
# 6. Gestão de membros do grupo de trackers
# ---------------------------------------------------------------------------


class TrackerRejoin(BaseModel):
    """Novo tracker se apresentando ao bootstrap node (tracker -> tracker)."""

    type: Literal["TRACKER_REJOIN"] = "TRACKER_REJOIN"
    tracker_id: str
    ip: str
    porta: int


class TrackerAnnounceNew(BaseModel):
    """Descritor do novo tracker dentro de TRACKER_ANNOUNCE."""

    tracker_id: str
    ip: str
    porta: int


class TrackerAnnounce(BaseModel):
    """Anúncio de novo tracker aos demais (tracker -> tracker, flooding TCP)."""

    type: Literal["TRACKER_ANNOUNCE"] = "TRACKER_ANNOUNCE"
    novo_tracker: TrackerAnnounceNew


class ReassignTracker(BaseModel):
    """Reatribuição de peer ao tracker reintegrado (tracker ativo -> peer)."""

    type: Literal["REASSIGN_TRACKER"] = "REASSIGN_TRACKER"
    peer_nome: str
    novo_tracker_ip: str
    novo_tracker_porta: int


# ---------------------------------------------------------------------------
# 7. Sinalização de erro genérica
# ---------------------------------------------------------------------------


class ErrorMessage(BaseModel):
    """Resposta de erro a qualquer requisição (qualquer -> qualquer)."""

    type: Literal["ERROR"] = "ERROR"
    ref_type: str
    ref_id: str
    code: str
    mensagem: str


# ---------------------------------------------------------------------------
# Registro de tipos válidos e validação em runtime
# ---------------------------------------------------------------------------

#: Mapa de ``type`` -> modelo pydantic correspondente (os 19 do Listing 7.2).
MESSAGE_MODELS: dict[str, type[BaseModel]] = {
    "PEER_HELLO": PeerHello,
    "PEER_LEAVE": PeerLeave,
    "UPDATE_IP": UpdateIp,
    "SEED_REPORT": SeedReport,
    "REGISTER_FILE": RegisterFile,
    "SEARCH_FILE": SearchFile,
    "SEARCH_FORWARD": SearchForward,
    "SEARCH_RESULT": SearchResult,
    "CHUNK_LIST_REQUEST": ChunkListRequest,
    "CHUNK_LIST": ChunkList,
    "CHUNK_REQUEST": ChunkRequest,
    "CHUNK_DATA": ChunkDataHeader,
    "PEER_LEAVE_FILE": PeerLeaveFile,
    "SYNC_TABLE": SyncTable,
    "FULL_SYNC": FullSync,
    "TRACKER_REJOIN": TrackerRejoin,
    "TRACKER_ANNOUNCE": TrackerAnnounce,
    "REASSIGN_TRACKER": ReassignTracker,
    "ERROR": ErrorMessage,
}


def _campos_obrigatorios(modelo: type[BaseModel]) -> tuple[str, ...]:
    return tuple(
        nome for nome, campo in modelo.model_fields.items() if campo.is_required()
    )


#: Mapa de ``type`` -> campos obrigatórios, derivado dos modelos pydantic.
#: (``type`` tem default e por isso não aparece; idem aos opcionais de
#: re-registro do REGISTER_FILE.)
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    tipo: _campos_obrigatorios(modelo) for tipo, modelo in MESSAGE_MODELS.items()
}


def validate_message(msg: dict[str, Any]) -> str:
    """Valida ``msg`` contra o modelo pydantic do seu ``type``.

    Args:
        msg: Dicionário recém-decodificado do JSON.

    Returns:
        O ``type`` da mensagem.

    Raises:
        ValueError: Se ``type`` estiver ausente ou for desconhecido, se
            faltar campo obrigatório, ou se algum campo tiver tipo inválido.
    """
    if not isinstance(msg, dict):
        raise ValueError(f"Mensagem deve ser dict; recebido {type(msg).__name__}")
    msg_type = msg.get("type")
    if not isinstance(msg_type, str):
        raise ValueError("Mensagem sem campo 'type' válido")
    modelo = MESSAGE_MODELS.get(msg_type)
    if modelo is None:
        raise ValueError(f"Tipo de mensagem desconhecido: {msg_type!r}")
    _validar_com_modelo(modelo, msg, msg_type)
    return msg_type


def _validar_com_modelo(
    modelo: type[BaseModel], msg: dict[str, Any], msg_type: str
) -> None:
    try:
        modelo.model_validate(msg)
    except ValidationError as exc:
        faltando = [
            ".".join(str(parte) for parte in erro["loc"])
            for erro in exc.errors()
            if erro["type"] == "missing"
        ]
        if faltando:
            raise ValueError(
                f"Mensagem {msg_type} sem campos obrigatórios: {faltando}"
            ) from exc
        raise ValueError(f"Mensagem {msg_type} inválida: {exc}") from exc
