"""Índice em memória do tracker.

Mantém as tabelas nome→hashes, hash→metadata, hash→peers,
nome_peer→endereço e os tombstones. O índice não é persistido:
ao reiniciar, o tracker o reconstrói via SYNC_PULL(desde_seq=0)
(reintegração) e via SEED_REPORT dos peers.

Além do estado replicado, o índice guarda a proveniência de cada
escrita — o par (origem, seq) — e um vetor de versões visto
(maior seq já visto por origem). O seq só DETECTA deltas perdidos
no flooding; o desempate de conflito continua sendo LWW por timestamp.
O contador local visto[tracker_id] (== meu_seq) e
as pendências de pull vivem sob o MESMO lock do índice.

Todo método público adquire self._lock. Métodos com sufixo
_locked assumem que o lock já está adquirido e nunca devem ser
chamados de fora desta classe.
"""

from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src.common.errors import NotFoundError, PeerUnknownError
from src.common.messages import (
    SearchResultEntry,
    SearchResultPeer,
    SyncPullItem,
    SyncTable,
    SyncTableEntry,
)


@dataclass
class FileMetadata:
    """Metadados imutáveis de um arquivo registrado (do REGISTER_FILE inicial)."""

    nome: str
    tamanho: int
    n_chunks: int


@dataclass
class PeerEntry:
    """Um peer como fonte de um hash, com proveniência para LWW/seq.

    origem/seq identificam a escrita: o tracker que a produziu e o
    seu contador monotônico no instante. origem desempata o LWW quando
    timestamps colidem — comparar contra o tracker LOCAL
    não bastaria, o vencedor dependeria da ordem de chegada e as réplicas
    divergiriam. seq só DETECTA deltas perdidos (vetor de versões /
    SYNC_PULL), nunca desempata conflito. Ambos são sempre preenchidos:
    escrita local carimba (tracker_id, meu_seq); escrita remota carrega
    o (origem, seq) da mensagem.
    """

    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    timestamp: float
    origem: str
    seq: int


@dataclass
class PeerAddress:
    """Endereço corrente de um peer + instante do último SEED_REPORT."""

    ip: str
    porta: int
    last_seed_ts: float


@dataclass
class TombstoneEntry:
    """Remoção registrada de (hash, peer); expira após 10 min.

    origem/seq têm o mesmo papel do PeerEntry: desempate LWW
    por origem, detecção de perda por seq (nunca desempate).
    """

    nome_peer: str
    ip: str
    porta: int
    timestamp: float
    origem: str
    seq: int


@dataclass
class IndexSnapshot:
    """Cópia profunda e consistente do estado do índice (para status/testes)."""

    nome_to_hashes: dict[str, set[str]]
    hash_to_metadata: dict[str, FileMetadata]
    hash_to_peers: dict[str, dict[str, PeerEntry]]
    nome_peer_to_endereco: dict[str, PeerAddress]
    tombstones: dict[str, dict[str, TombstoneEntry]]


@dataclass
class LocalDelta:
    """Entradas de UM evento local (mesmo seq e timestamp) prontas para flooding.

    Um evento — reconciliação de SEED_REPORT, saída ordenada (PEER_LEAVE)
    ou detecção de falha — pode tocar vários pares (hash, peer), mas consome um
    único seq e um único timestamp. Esse timestamp compartilhado é
    obrigatório: a SYNC_TABLE de propagação o carrega no nível da mensagem,
    então TODAS as entradas gravadas localmente precisam do MESMO valor, ou o LWW
    divergiria entre réplicas.
    """

    seq: int
    timestamp: float
    entries: list[SyncTableEntry]


class Index:
    """Estado em memória do tracker, protegido por um único threading.Lock.

    O relógio é injetado por construtor para testes determinísticos;
    tracker_id identifica este tracker como origem das escritas
    locais, usado no desempate do LWW:

        >>> indice = Index(clock=lambda: 1000.0, tracker_id="tracker-1")
        >>> indice.register_peer("alice", "127.0.0.1", 7001)
    """

    def __init__(
        self, clock: Callable[[], float] = time.time, tracker_id: str = ""
    ) -> None:
        self._lock = threading.Lock()
        self._clock = clock
        self._tracker_id = tracker_id
        self.nome_to_hashes: dict[str, set[str]] = {}
        self.hash_to_metadata: dict[str, FileMetadata] = {}
        self.hash_to_peers: dict[str, dict[str, PeerEntry]] = {}
        self.nome_peer_to_endereco: dict[str, PeerAddress] = {}
        self.tombstones: dict[str, dict[str, TombstoneEntry]] = {}
        # Proveniencia e deteccao de perda, sob o MESMO lock.
        # visto[origem] = maior seq ja visto/aplicado daquela origem;
        # visto[tracker_id] e o proprio meu_seq (contador de escritas locais).
        # pendencias[origem] = desde_seq capturado numa lacuna, aguardando a
        # resposta do SYNC_PULL (fecha quando a resposta chega).
        self._visto: dict[str, int] = {}
        self._pendencias: dict[str, int] = {}
        # Rebalance: peers locais agendados para migrar a outro tracker.
        # nome_peer -> (novo_ip, nova_api_port); entregue ao peer como campo
        # 'reassign_to' na resposta REST da sua próxima chamada.
        self._reassign_pendente: dict[str, tuple[str, int]] = {}

    # ------------------------------------------------------------------
    # Presença de peers
    # ------------------------------------------------------------------

    def register_peer(self, nome_peer: str, ip: str, porta: int) -> None:
        """Registra (ou re-registra) a presença de um peer.

        Usado pelo PEER_HELLO e também pelo SEED_REPORT, que carrega
        ip/porta justamente para reconstruir o índice após restart
        do tracker.
        """
        with self._lock:
            self.nome_peer_to_endereco[nome_peer] = PeerAddress(
                ip=ip, porta=porta, last_seed_ts=self._clock()
            )

    def remove_peer(self, nome_peer: str) -> LocalDelta | None:
        """Saída ordenada (PEER_LEAVE): some o endereço e tombstona tudo.

        Returns:
            O LocalDelta com os tombstones gerados (um só seq/timestamp)
            para o chamador propagar via SYNC_TABLE, ou None se o peer não
            era fonte de nenhum arquivo.

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            self._endereco_ou_erro_locked(nome_peer)
            del self.nome_peer_to_endereco[nome_peer]
            hashes = [
                h for h, fontes in self.hash_to_peers.items() if nome_peer in fontes
            ]
            if not hashes:
                return None
            # Um unico evento (uma saida) -> um seq e um timestamp compartilhados.
            ts = self._clock()
            seq = self._proximo_seq_local_locked()
            entries = [
                self._tombstonar_para_delta_locked(h, nome_peer, seq=seq, timestamp=ts)
                for h in hashes
            ]
            return LocalDelta(seq=seq, timestamp=ts, entries=entries)

    def update_peer_address(self, nome_peer: str, novo_ip: str, porta: int) -> None:
        """Aplica UPDATE_IP: novo endereço refletido em todas as fontes.

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            endereco.ip = novo_ip
            endereco.porta = porta
            self._atualizar_fontes_locked(nome_peer, novo_ip, porta)

    def update_last_seed(self, nome_peer: str) -> None:
        """Renova o sinal de vida do peer (base do failure detector).

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            self._endereco_ou_erro_locked(nome_peer).last_seed_ts = self._clock()

    # ------------------------------------------------------------------
    # Registro e remoção de arquivos
    # ------------------------------------------------------------------

    def register_file(
        self,
        nome_peer: str,
        hash_arquivo: str,
        nome: str | None = None,
        tamanho: int | None = None,
        n_chunks: int | None = None,
    ) -> tuple[PeerEntry, FileMetadata]:
        """Aplica REGISTER_FILE: upload original ou re-registro pós-download.

        No re-registro, nome/tamanho/n_chunks são opcionais — o
        tracker já os conhece do upload original.

        Returns:
            Cópias da entrada gravada e dos metadados do arquivo, para que o
            chamador monte o SYNC_TABLE de propagação com o MESMO
            timestamp e seq gravados localmente (LWW exige timestamps
            idênticos entre as réplicas; o seq viaja no nível da mensagem).

        Raises:
            PeerUnknownError: Se o peer não enviou PEER_HELLO antes.
            NotFoundError: Se o hash é desconhecido e os metadados faltam.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            self._garantir_metadata_locked(hash_arquivo, nome, tamanho, n_chunks)
            seq = self._proximo_seq_local_locked()
            self._registrar_fonte_locked(
                hash_arquivo, nome_peer, endereco, refresh=True, seq=seq
            )
            return (
                copy.copy(self.hash_to_peers[hash_arquivo][nome_peer]),
                copy.copy(self.hash_to_metadata[hash_arquivo]),
            )

    def remove_peer_from_hash(
        self, hash_arquivo: str, nome_peer: str
    ) -> TombstoneEntry:
        """Aplica PEER_LEAVE_FILE: o par (hash, peer) vira tombstone.

        Returns:
            Cópia do tombstone gravado, para propagação via SYNC_TABLE
            com o mesmo timestamp e seq locais (LWW).

        Raises:
            NotFoundError: Se o peer não consta como fonte do hash.
        """
        with self._lock:
            seq = self._proximo_seq_local_locked()
            self._tombstone_locked(hash_arquivo, nome_peer, seq=seq)
            return copy.copy(self.tombstones[hash_arquivo][nome_peer])

    def apply_seed_hashes(self, nome_peer: str, hashes: set[str]) -> LocalDelta | None:
        """Anti-entropy do SEED_REPORT: hash omitido equivale a remoção.

        Hashes reportados que o índice já conhece (metadata presente) ganham
        o peer como fonte; hashes que o índice atribuía ao peer mas sumiram
        do relatório viram tombstone (PEER_LEAVE_FILE é
        redundante com isso, mas dá resposta imediata).

        Um relatório sem mudanças (estado estacionário) não consome seq —
        só uma reconciliação que de fato altera o índice avança meu_seq,
        evitando churn no vetor de versões a cada 3 minutos.

        Returns:
            O LocalDelta com as entradas alteradas (registros com metadados
            e/ou tombstones, sob um só seq/timestamp) para o chamador
            propagar via SYNC_TABLE, ou None se nada mudou. A detecção
            por seq/digest é backstop caso a propagação se perca.

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            novas = {
                h
                for h in hashes & self.hash_to_metadata.keys()
                if nome_peer not in self.hash_to_peers.get(h, {})
            }
            atuais = {
                h for h, fontes in self.hash_to_peers.items() if nome_peer in fontes
            }
            removidas = atuais - hashes
            if not novas and not removidas:
                return None
            # Uma reconciliacao (um SEED_REPORT) -> um seq e um timestamp.
            ts = self._clock()
            seq = self._proximo_seq_local_locked()
            entries: list[SyncTableEntry] = []
            for hash_arquivo in novas:
                self._registrar_fonte_locked(
                    hash_arquivo,
                    nome_peer,
                    endereco,
                    refresh=False,
                    seq=seq,
                    timestamp=ts,
                )
                meta = self.hash_to_metadata[hash_arquivo]
                entries.append(
                    SyncTableEntry(
                        hash=hash_arquivo,
                        nome_peer=nome_peer,
                        ip=endereco.ip,
                        porta=endereco.porta,
                        ativo=True,
                        nome=meta.nome,
                        tamanho=meta.tamanho,
                        n_chunks=meta.n_chunks,
                    )
                )
            for hash_arquivo in removidas:
                entries.append(
                    self._tombstonar_para_delta_locked(
                        hash_arquivo, nome_peer, seq=seq, timestamp=ts
                    )
                )
            return LocalDelta(seq=seq, timestamp=ts, entries=entries)

    # ------------------------------------------------------------------
    # Detecção de falha de peer e rebalance
    # ------------------------------------------------------------------

    def detectar_peers_falhos(
        self, timeout_seconds: float
    ) -> list[tuple[str, LocalDelta | None]]:
        """Tombstona peers sem SEED_REPORT há mais de timeout_seconds.

        Um peer cujo last_seed_ts é mais antigo que clock() -
        timeout_seconds (default 360s = 2 rodadas) é considerado falho: sai de
        nome_peer_to_endereco e todas as suas fontes viram tombstone, sob um
        único seq/timestamp (um evento por peer). Chamado pela thread de
        src.tracker.failure_detector.

        Returns:
            Uma tupla (nome_peer, delta) por peer considerado falho: delta
            traz os tombstones a propagar, ou None se o peer não era fonte de
            nenhum arquivo (apenas presença expirada).
        """
        with self._lock:
            limite = self._clock() - timeout_seconds
            falhos = [
                nome
                for nome, endereco in self.nome_peer_to_endereco.items()
                if endereco.last_seed_ts < limite
            ]
            resultado: list[tuple[str, LocalDelta | None]] = []
            for nome_peer in falhos:
                del self.nome_peer_to_endereco[nome_peer]
                resultado.append((nome_peer, self._tombstonar_peer_locked(nome_peer)))
            return resultado

    def listar_peers_locais(self) -> list[str]:
        """Nomes dos peers com presença ativa neste tracker (base do rebalance)."""
        with self._lock:
            return sorted(self.nome_peer_to_endereco)

    def agendar_reassign(
        self, nome_peer: str, novo_ip: str, nova_api_port: int
    ) -> None:
        """Agenda a migração de nome_peer para outro tracker (rebalance).

        A migração é entregue ao peer como reassign_to na resposta da sua
        próxima chamada REST (simplificação aceita: sem push TCP ao peer).
        """
        with self._lock:
            self._reassign_pendente[nome_peer] = (novo_ip, nova_api_port)

    def consumir_reassign(self, nome_peer: str) -> tuple[str, int] | None:
        """Retira (uma única vez) a migração pendente de nome_peer, se houver."""
        with self._lock:
            return self._reassign_pendente.pop(nome_peer, None)
        
    def remover_peer_local(self, nome_peer: str) -> bool:
        """Removo o peer da lista de peers locais"""
        with self._lock:
            return True if self.nome_peer_to_endereco.pop(nome_peer, False) != False else False
            

    # ------------------------------------------------------------------
    # Consultas
    # ------------------------------------------------------------------

    def search_by_name(self, query: str) -> list[SearchResultEntry]:
        """Busca por nome na tabela nome→hashes (fluxo de busca).

        Casa tanto o nome completo (com extensão) quanto o seu stem, para que
        'Imagine' encontre 'Imagine.mp3'. Um nome pode mapear para múltiplos
        hashes (versões distintas); hashes sem nenhuma fonte ativa são omitidos.
        """
        with self._lock:
            hashes: set[str] = set()
            for nome, hs in self.nome_to_hashes.items():
                if nome == query or Path(nome).stem == query:
                    hashes.update(hs)
            resultados: list[SearchResultEntry] = []
            for hash_arquivo in sorted(hashes):
                peers = self._peers_ativos_locked(hash_arquivo)
                if not peers:
                    continue
                meta = self.hash_to_metadata[hash_arquivo]
                resultados.append(
                    SearchResultEntry(
                        hash=hash_arquivo,
                        nome=meta.nome,
                        n_chunks=meta.n_chunks,
                        peers=peers,
                    )
                )
            return resultados

    def conhece_hash(self, hash_arquivo: str) -> bool:
        """Indica se o hash tem metadados no índice (arquivo registrado na rede).

        Usado para validar itens de playlist: só se pode adicionar à playlist um
        hash de um arquivo que o tracker conhece (metadados são replicados via
        SYNC_TABLE), não uma string arbitrária.
        """
        with self._lock:
            return hash_arquivo in self.hash_to_metadata

    def get_peers_for_hash(self, hash_arquivo: str) -> list[SearchResultPeer]:
        """Lista as fontes ativas de um hash (comando peers <hash> da CLI).

        Raises:
            NotFoundError: Se o hash não consta no índice.
        """
        with self._lock:
            if hash_arquivo not in self.hash_to_metadata:
                raise NotFoundError(f"hash {hash_arquivo!r} não consta no índice")
            return self._peers_ativos_locked(hash_arquivo)

    def get_snapshot(self) -> IndexSnapshot:
        """Devolve uma cópia profunda e consistente de todo o estado."""
        with self._lock:
            return IndexSnapshot(
                nome_to_hashes=copy.deepcopy(self.nome_to_hashes),
                hash_to_metadata=copy.deepcopy(self.hash_to_metadata),
                hash_to_peers=copy.deepcopy(self.hash_to_peers),
                nome_peer_to_endereco=copy.deepcopy(self.nome_peer_to_endereco),
                tombstones=copy.deepcopy(self.tombstones),
            )

    # ------------------------------------------------------------------
    # Sincronização entre trackers: LWW, seq e reparo direcionado
    # ------------------------------------------------------------------

    def apply_sync_entry(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float, seq: int
    ) -> bool:
        """Aplica uma entrada de SYNC_TABLE com resolução LWW.

        timestamp e seq vêm do nível da mensagem SYNC_TABLE
        (são definidos por mensagem, não por entry) e por isso são
        parâmetros. O seq é gravado como proveniência da entrada (usado
        pelo vetor de versões e pelo SYNC_PULL); ele NÃO entra na decisão
        LWW — só o par (timestamp, origem) decide.

        Regras:
        * timestamp recebido maior que o local → recebido vence;
        * menor → descartado como desatualizado;
        * empate → vence o maior tracker_id (lexicográfico), comparando
          a origem da escrita local com origem_tracker — desempate
          determinístico em todas as réplicas, independente da ordem de
          chegada;
        * ativo=False vira tombstone; ativo=True sobre tombstone
          mais antigo remove o tombstone e registra a fonte.

        A tabela nome_peer_to_endereco NÃO é tocada: presença e
        failure detection (last_seed_ts) são responsabilidade do
        tracker ao qual o peer reporta SEED_REPORT; a própria entry
        carrega ip/porta, suficiente para responder buscas.

        O vetor de versões visto avança à parte, no nível da mensagem
        (registrar_recepcao_flood / avancar_visto), pois o seq é
        por mensagem e o avanço ocorre mesmo quando o LWW descarta a entrada.

        Returns:
            True se a entrada foi aplicada, False se descartada
            pelo LWW.
        """
        with self._lock:
            versao_local = self._versao_local_locked(entry.hash, entry.nome_peer)
            if versao_local is not None and (timestamp, origem_tracker) <= versao_local:
                return False  # LWW: desatualizada (empate vence maior tracker_id)
            if entry.ativo:
                self._aplicar_registro_remoto_locked(
                    entry, origem_tracker, timestamp, seq
                )
            else:
                self._aplicar_tombstone_remoto_locked(
                    entry, origem_tracker, timestamp, seq
                )
            return True

    def registrar_recepcao_flood(self, origem: str, seq: int) -> int | None:
        """Registra o seq de um SYNC_TABLE do flooding e detecta lacunas.

        Avança visto[origem] por max (mesmo com lacuna aberta) e, se
        houver buraco (seq > visto[origem] + 1), abre/atualiza uma
        pendência guardando o visto[origem] capturado NO INSTANTE da
        detecção — não o valor já avançado (o "Estado de
        pendência"). Por origem, mantém o MENOR desde_seq (um pull desde o
        menor já cobre qualquer buraco acima).

        Returns:
            O desde_seq a pedir num SYNC_PULL se uma lacuna foi
            detectada; None se o seq for contíguo.
        """
        with self._lock:
            anterior = self._visto.get(origem, 0)
            self._visto[origem] = max(anterior, seq)
            if seq <= anterior + 1:
                return None
            desde = min(self._pendencias.get(origem, anterior), anterior)
            self._pendencias[origem] = desde
            return desde

    def avancar_visto(self, origem: str, seq: int) -> None:
        """Avança visto[origem] por max SEM detecção de lacuna.

        Usado ao aplicar RESPOSTAS de SYNC_PULL (o próprio reparo) e na
        reconstrução por reintegração: reaplicar deltas não deve abrir novas
        pendências nem disparar novos pulls.
        """
        with self._lock:
            self._visto[origem] = max(self._visto.get(origem, 0), seq)

    def resolver_pendencia(self, origem: str) -> None:
        """Fecha a pendência de origem — a resposta do SYNC_PULL chegou.

        Idempotente e re-disparável: se o reparo não bastou, a próxima escrita
        de origem (detecção inline) ou o SYNC_DIGEST reabre o pedido.
        """
        with self._lock:
            self._pendencias.pop(origem, None)

    def versoes(self) -> dict[str, int]:
        """Vetor de versões visto (cópia), incluindo versoes[meu_id].

        É o payload do SYNC_DIGEST: o maior seq conhecido por origem,
        com o próprio meu_seq sempre presente (mesmo que 0).
        """
        with self._lock:
            copia = dict(self._visto)
            copia.setdefault(self._tracker_id, 0)
            return copia

    def pendencias(self) -> dict[str, int]:
        """Pendências de pull em aberto (cópia) — para status/observabilidade."""
        with self._lock:
            return dict(self._pendencias)

    def comparar_digest(self, versoes_remoto: dict[str, int]) -> list[SyncPullItem]:
        """Compara um SYNC_DIGEST recebido com o visto local.

        Para cada origem em que o emissor está à frente (versoes_remoto >
        visto local), devolve um SyncPullItem(origem, desde_seq=visto
        local) a pedir ao emissor. Fecha o ponto cego da detecção inline
        (última escrita perdida + silêncio da origem).
        """
        with self._lock:
            faltando: list[SyncPullItem] = []
            for origem, seq_remoto in versoes_remoto.items():
                local = self._visto.get(origem, 0)
                if seq_remoto > local:
                    faltando.append(SyncPullItem(origem=origem, desde_seq=local))
            return faltando

    def selecionar_para_pull(self, faltando: list[SyncPullItem]) -> list[SyncTable]:
        """Monta a resposta de um SYNC_PULL a partir do ESTADO ATUAL.

        Para cada {origem, desde_seq} pedido, seleciona as entradas cuja
        proveniência tem essa origem e seq > desde_seq (fontes ativas e
        tombstones), agrupa por seq e devolve uma SYNC_TABLE por evento
        (um seq por mensagem). Sem log histórico: cada entrada já carrega a
        (origem, seq, timestamp) da escrita corrente e o LWW no receptor
        descarta o que estiver velho. desde_seq=0 devolve
        o estado inteiro daquela origem (reconstrução na reintegração).
        """
        with self._lock:
            mensagens: list[SyncTable] = []
            for item in faltando:
                mensagens.extend(
                    self._eventos_da_origem_locked(item.origem, item.desde_seq)
                )
            return mensagens

    def expire_tombstones(self, retention_seconds: float) -> int:
        """Descarta tombstones mais velhos que retention_seconds.

        Chamado periodicamente pela thread de src.tracker.tombstone.

        Returns:
            Quantidade de tombstones removidos.
        """
        with self._lock:
            limite = self._clock() - retention_seconds
            removidos = 0
            for hash_arquivo in list(self.tombstones):
                por_peer = self.tombstones[hash_arquivo]
                for nome_peer in [
                    p for p, t in por_peer.items() if t.timestamp < limite
                ]:
                    del por_peer[nome_peer]
                    removidos += 1
                if not por_peer:
                    del self.tombstones[hash_arquivo]
            return removidos

    # ------------------------------------------------------------------
    # Helpers privados — exigem o lock já adquirido
    # ------------------------------------------------------------------

    def _endereco_ou_erro_locked(self, nome_peer: str) -> PeerAddress:
        endereco = self.nome_peer_to_endereco.get(nome_peer)
        if endereco is None:
            raise PeerUnknownError(
                f"peer {nome_peer!r} desconhecido — envie PEER_HELLO antes"
            )
        return endereco

    def _garantir_metadata_locked(
        self,
        hash_arquivo: str,
        nome: str | None,
        tamanho: int | None,
        n_chunks: int | None,
    ) -> None:
        if hash_arquivo in self.hash_to_metadata:
            return  # re-registro: metadados já conhecidos do upload original
        if nome is None or tamanho is None or n_chunks is None:
            raise NotFoundError(
                f"hash {hash_arquivo!r} desconhecido; o REGISTER_FILE inicial "
                "exige nome, tamanho e n_chunks"
            )
        self.hash_to_metadata[hash_arquivo] = FileMetadata(
            nome=nome, tamanho=tamanho, n_chunks=n_chunks
        )
        self.nome_to_hashes.setdefault(nome, set()).add(hash_arquivo)

    def _proximo_seq_local_locked(self) -> int:
        """Aloca o próximo seq para uma escrita ORIGINADA por este tracker.

        Incrementa meu_seq (mantido em visto[tracker_id]) e o devolve.
        Um evento local — registro, tombstone, UPDATE_IP ou reconciliação de
        SEED_REPORT — consome UM seq, carimbando todas as entradas
        afetadas.
        """
        novo = self._visto.get(self._tracker_id, 0) + 1
        self._visto[self._tracker_id] = novo
        return novo

    def _registrar_fonte_locked(
        self,
        hash_arquivo: str,
        nome_peer: str,
        endereco: PeerAddress,
        *,
        refresh: bool,
        seq: int,
        timestamp: float | None = None,
    ) -> None:
        """Adiciona o peer como fonte; refresh=False preserva o timestamp.

        REGISTER_FILE explícito usa refresh=True (evento novo para LWW);
        o anti-entropy do SEED_REPORT usa refresh=False para não gerar
        churn de timestamp a cada 3 minutos. seq é a proveniência local
        já alocada pelo chamador (um por evento); timestamp, quando dado,
        garante que todas as entradas de um mesmo evento compartilhem o valor
        que viajará na SYNC_TABLE (LWW convergente entre réplicas).
        """
        fontes = self.hash_to_peers.setdefault(hash_arquivo, {})
        if not refresh and nome_peer in fontes:
            return
        fontes[nome_peer] = PeerEntry(
            nome_peer=nome_peer,
            ip=endereco.ip,
            porta=endereco.porta,
            ativo=True,
            timestamp=self._clock() if timestamp is None else timestamp,
            origem=self._tracker_id,
            seq=seq,
        )
        self._descartar_tombstone_locked(hash_arquivo, nome_peer)

    def _tombstone_locked(
        self,
        hash_arquivo: str,
        nome_peer: str,
        seq: int,
        timestamp: float | None = None,
    ) -> None:
        entry = self.hash_to_peers.get(hash_arquivo, {}).pop(nome_peer, None)
        if entry is None:
            raise NotFoundError(
                f"peer {nome_peer!r} não é fonte do hash {hash_arquivo!r}"
            )
        self.tombstones.setdefault(hash_arquivo, {})[nome_peer] = TombstoneEntry(
            nome_peer=nome_peer,
            ip=entry.ip,
            porta=entry.porta,
            timestamp=self._clock() if timestamp is None else timestamp,
            origem=self._tracker_id,
            seq=seq,
        )

    def _tombstonar_para_delta_locked(
        self, hash_arquivo: str, nome_peer: str, *, seq: int, timestamp: float
    ) -> SyncTableEntry:
        """Tombstona (hash, peer) e devolve a SyncTableEntry (ativo=False).

        Reúne o tombstone e a entrada de propagação num passo, para eventos
        multi-hash montarem seu LocalDelta sem reconsultar a tabela.
        """
        self._tombstone_locked(hash_arquivo, nome_peer, seq=seq, timestamp=timestamp)
        tomb = self.tombstones[hash_arquivo][nome_peer]
        return SyncTableEntry(
            hash=hash_arquivo,
            nome_peer=nome_peer,
            ip=tomb.ip,
            porta=tomb.porta,
            ativo=False,
        )

    def _tombstonar_peer_locked(self, nome_peer: str) -> LocalDelta | None:
        """Tombstona todas as fontes de nome_peer sob um só seq/timestamp."""
        hashes = [h for h, fontes in self.hash_to_peers.items() if nome_peer in fontes]
        if not hashes:
            return None
        ts = self._clock()
        seq = self._proximo_seq_local_locked()
        entries = [
            self._tombstonar_para_delta_locked(h, nome_peer, seq=seq, timestamp=ts)
            for h in hashes
        ]
        return LocalDelta(seq=seq, timestamp=ts, entries=entries)

    def _atualizar_fontes_locked(self, nome_peer: str, ip: str, porta: int) -> None:
        agora = self._clock()
        seq: int | None = None
        for fontes in self.hash_to_peers.values():
            entry = fontes.get(nome_peer)
            if entry is None:
                continue
            if seq is None:
                # Um unico seq compartilhado por todo o UPDATE_IP (um evento).
                seq = self._proximo_seq_local_locked()
            entry.ip = ip
            entry.porta = porta
            entry.timestamp = agora
            entry.origem = self._tracker_id
            entry.seq = seq

    def _descartar_tombstone_locked(self, hash_arquivo: str, nome_peer: str) -> None:
        """Remove um tombstone sem deixar dict vazio órfão na tabela."""
        por_peer = self.tombstones.get(hash_arquivo)
        if por_peer is None:
            return
        por_peer.pop(nome_peer, None)
        if not por_peer:
            del self.tombstones[hash_arquivo]

    def _versao_local_locked(
        self, hash_arquivo: str, nome_peer: str
    ) -> tuple[float, str] | None:
        """Versão LWW local de (hash, peer): (timestamp, origem) ou None.

        A versão vigente está em hash_to_peers (fonte ativa) ou em
        tombstones (remoção) — nunca em ambos.
        """
        fonte = self.hash_to_peers.get(hash_arquivo, {}).get(nome_peer)
        if fonte is not None:
            return (fonte.timestamp, fonte.origem)
        tombstone = self.tombstones.get(hash_arquivo, {}).get(nome_peer)
        if tombstone is not None:
            return (tombstone.timestamp, tombstone.origem)
        return None

    def _aplicar_registro_remoto_locked(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float, seq: int
    ) -> None:
        if entry.hash not in self.hash_to_metadata and entry.nome is not None:
            # Metadados viajam no SYNC_TABLE para que este tracker
            # responda buscas por nome sem SEARCH_FORWARD.
            self._garantir_metadata_locked(
                entry.hash, entry.nome, entry.tamanho, entry.n_chunks
            )
        self._descartar_tombstone_locked(entry.hash, entry.nome_peer)
        self.hash_to_peers.setdefault(entry.hash, {})[entry.nome_peer] = PeerEntry(
            nome_peer=entry.nome_peer,
            ip=entry.ip,
            porta=entry.porta,
            ativo=True,
            timestamp=timestamp,
            origem=origem_tracker,
            seq=seq,
        )

    def _aplicar_tombstone_remoto_locked(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float, seq: int
    ) -> None:
        # Tombstone gravado mesmo sem fonte local prévia: protege contra um
        # registro atrasado (timestamp menor) que chegue depois da remoção.
        self.hash_to_peers.get(entry.hash, {}).pop(entry.nome_peer, None)
        self.tombstones.setdefault(entry.hash, {})[entry.nome_peer] = TombstoneEntry(
            nome_peer=entry.nome_peer,
            ip=entry.ip,
            porta=entry.porta,
            timestamp=timestamp,
            origem=origem_tracker,
            seq=seq,
        )

    def _eventos_da_origem_locked(self, origem: str, desde_seq: int) -> list[SyncTable]:
        """Agrupa por seq as entradas de origem com seq > desde_seq.

        Fontes ativas viram entries ativo=True (com metadados, para o
        receptor responder buscas por nome); tombstones viram ativo=False.
        Uma SYNC_TABLE por seq (um evento por mensagem).
        """
        por_seq: dict[int, tuple[float, list[SyncTableEntry]]] = {}
        for hash_arquivo, fontes in self.hash_to_peers.items():
            meta = self.hash_to_metadata.get(hash_arquivo)
            for fonte in fontes.values():
                if fonte.origem != origem or fonte.seq <= desde_seq:
                    continue
                entrada = SyncTableEntry(
                    hash=hash_arquivo,
                    nome_peer=fonte.nome_peer,
                    ip=fonte.ip,
                    porta=fonte.porta,
                    ativo=True,
                    nome=meta.nome if meta is not None else None,
                    tamanho=meta.tamanho if meta is not None else None,
                    n_chunks=meta.n_chunks if meta is not None else None,
                )
                por_seq.setdefault(fonte.seq, (fonte.timestamp, []))[1].append(entrada)
        for hash_arquivo, por_peer in self.tombstones.items():
            for tomb in por_peer.values():
                if tomb.origem != origem or tomb.seq <= desde_seq:
                    continue
                entrada = SyncTableEntry(
                    hash=hash_arquivo,
                    nome_peer=tomb.nome_peer,
                    ip=tomb.ip,
                    porta=tomb.porta,
                    ativo=False,
                )
                por_seq.setdefault(tomb.seq, (tomb.timestamp, []))[1].append(entrada)
        return [
            SyncTable(origem=origem, seq=seq, timestamp=ts, entries=entradas)
            for seq, (ts, entradas) in sorted(por_seq.items())
        ]

    def _peers_ativos_locked(self, hash_arquivo: str) -> list[SearchResultPeer]:
        return [
            SearchResultPeer(nome_peer=e.nome_peer, ip=e.ip, porta=e.porta)
            for e in self.hash_to_peers.get(hash_arquivo, {}).values()
            if e.ativo
        ]
