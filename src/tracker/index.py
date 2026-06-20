"""Índice em memória do tracker (§6.2 do CLAUDE.md).

Mantém as tabelas ``nome→hashes``, ``hash→metadata``, ``hash→peers``,
``nome_peer→endereço`` e os tombstones. O índice **não é persistido**
(§11.4): ao reiniciar, o tracker o reconstrói via ``FULL_SYNC`` e via
``SEED_REPORT`` dos peers.

Todo método público adquire ``self._lock`` (§4.5). Métodos com sufixo
``_locked`` assumem que o lock já está adquirido e nunca devem ser
chamados de fora desta classe.
"""

from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from src.common.errors import NotFoundError, PeerUnknownError
from src.common.messages import (
    FullSyncEntry,
    SearchResultEntry,
    SearchResultPeer,
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
    """Um peer como fonte de um hash, com timestamp para LWW (§6.2).

    ``origem`` registra o tracker que produziu a escrita: é o desempate
    do LWW quando timestamps colidem (main.tex §12.2). Comparar contra o
    tracker LOCAL não bastaria — o vencedor dependeria da ordem de
    chegada e as réplicas divergiriam.
    """

    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    timestamp: float
    origem: str = ""


@dataclass
class PeerAddress:
    """Endereço corrente de um peer + instante do último SEED_REPORT."""

    ip: str
    porta: int
    last_seed_ts: float


@dataclass
class TombstoneEntry:
    """Remoção registrada de (hash, peer); expira após 10 min.

    ``origem`` tem o mesmo papel de desempate LWW do :class:`PeerEntry`.
    """

    nome_peer: str
    ip: str
    porta: int
    timestamp: float
    origem: str = ""


@dataclass
class IndexSnapshot:
    """Cópia profunda e consistente do estado do índice (para status/testes)."""

    nome_to_hashes: dict[str, set[str]]
    hash_to_metadata: dict[str, FileMetadata]
    hash_to_peers: dict[str, dict[str, PeerEntry]]
    nome_peer_to_endereco: dict[str, PeerAddress]
    tombstones: dict[str, dict[str, TombstoneEntry]]


class Index:
    """Estado em memória do tracker, protegido por um único ``threading.Lock``.

    O relógio é injetado por construtor para testes determinísticos (§10);
    ``tracker_id`` identifica este tracker como ``origem`` das escritas
    locais, usado no desempate do LWW (main.tex §12.2):

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

    # ------------------------------------------------------------------
    # Presença de peers
    # ------------------------------------------------------------------

    def register_peer(self, nome_peer: str, ip: str, porta: int) -> None:
        """Registra (ou re-registra) a presença de um peer.

        Usado pelo ``PEER_HELLO`` e também pelo ``SEED_REPORT``, que carrega
        ``ip``/``porta`` justamente para reconstruir o índice após restart
        do tracker (main.tex §7.2).
        """
        with self._lock:
            self.nome_peer_to_endereco[nome_peer] = PeerAddress(
                ip=ip, porta=porta, last_seed_ts=self._clock()
            )

    def remove_peer(self, nome_peer: str) -> None:
        """Saída ordenada (``PEER_LEAVE``): some o endereço e tombstona tudo.

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            self._endereco_ou_erro_locked(nome_peer)
            del self.nome_peer_to_endereco[nome_peer]
            hashes = [
                h for h, fontes in self.hash_to_peers.items() if nome_peer in fontes
            ]
            for hash_arquivo in hashes:
                self._tombstone_locked(hash_arquivo, nome_peer)

    def update_peer_address(self, nome_peer: str, novo_ip: str, porta: int) -> None:
        """Aplica ``UPDATE_IP``: novo endereço refletido em todas as fontes.

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            endereco.ip = novo_ip
            endereco.porta = porta
            self._atualizar_fontes_locked(nome_peer, novo_ip, porta)

    def update_last_seed(self, nome_peer: str) -> None:
        """Renova o sinal de vida do peer (base do failure detector, Fase 5).

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
        """Aplica ``REGISTER_FILE``: upload original ou re-registro pós-download.

        No re-registro, ``nome``/``tamanho``/``n_chunks`` são opcionais — o
        tracker já os conhece do upload original (main.tex §7.2).

        Returns:
            Cópias da entrada gravada e dos metadados do arquivo, para que o
            chamador monte o ``SYNC_TABLE`` de propagação com o MESMO
            timestamp gravado localmente (LWW exige timestamps idênticos
            entre as réplicas).

        Raises:
            PeerUnknownError: Se o peer não enviou ``PEER_HELLO`` antes.
            NotFoundError: Se o hash é desconhecido e os metadados faltam.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            self._garantir_metadata_locked(hash_arquivo, nome, tamanho, n_chunks)
            self._registrar_fonte_locked(
                hash_arquivo, nome_peer, endereco, refresh=True
            )
            return (
                copy.copy(self.hash_to_peers[hash_arquivo][nome_peer]),
                copy.copy(self.hash_to_metadata[hash_arquivo]),
            )

    def remove_peer_from_hash(
        self, hash_arquivo: str, nome_peer: str
    ) -> TombstoneEntry:
        """Aplica ``PEER_LEAVE_FILE``: o par (hash, peer) vira tombstone.

        Returns:
            Cópia do tombstone gravado, para propagação via ``SYNC_TABLE``
            com o mesmo timestamp local (LWW).

        Raises:
            NotFoundError: Se o peer não consta como fonte do hash.
        """
        with self._lock:
            self._tombstone_locked(hash_arquivo, nome_peer)
            return copy.copy(self.tombstones[hash_arquivo][nome_peer])

    def apply_seed_hashes(self, nome_peer: str, hashes: set[str]) -> None:
        """Anti-entropy do ``SEED_REPORT``: hash omitido equivale a remoção.

        Hashes reportados que o índice já conhece (metadata presente) ganham
        o peer como fonte; hashes que o índice atribuía ao peer mas sumiram
        do relatório viram tombstone — main.tex §7.2 (PEER_LEAVE_FILE é
        redundante com isso, mas dá resposta imediata).

        Raises:
            PeerUnknownError: Se o peer não estiver registrado.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            for hash_arquivo in hashes & self.hash_to_metadata.keys():
                self._registrar_fonte_locked(
                    hash_arquivo, nome_peer, endereco, refresh=False
                )
            atuais = {
                h for h, fontes in self.hash_to_peers.items() if nome_peer in fontes
            }
            for hash_arquivo in atuais - hashes:
                self._tombstone_locked(hash_arquivo, nome_peer)

    # ------------------------------------------------------------------
    # Consultas
    # ------------------------------------------------------------------

    def search_by_name(self, query: str) -> list[SearchResultEntry]:
        """Busca exata na tabela ``nome→hashes`` (main.tex, fluxo de busca).

        Um nome pode mapear para múltiplos hashes (versões distintas);
        hashes sem nenhuma fonte ativa são omitidos do resultado.
        """
        with self._lock:
            resultados: list[SearchResultEntry] = []
            for hash_arquivo in sorted(self.nome_to_hashes.get(query, set())):
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

    def get_peers_for_hash(self, hash_arquivo: str) -> list[SearchResultPeer]:
        """Lista as fontes ativas de um hash (comando ``peers <hash>`` da CLI).

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
    # Sincronização entre trackers (Fase 4)
    # ------------------------------------------------------------------

    def apply_sync_entry(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float
    ) -> bool:
        """Aplica uma entrada de ``SYNC_TABLE`` com resolução LWW (§6.2).

        ``timestamp`` vem do nível da mensagem ``SYNC_TABLE`` (Listing 7.2
        o define por mensagem, não por entry) e por isso é parâmetro.

        Regras (main.tex §12.2):
        * timestamp recebido maior que o local → recebido vence;
        * menor → descartado como desatualizado;
        * empate → vence o maior ``tracker_id`` (lexicográfico), comparando
          a ``origem`` da escrita local com ``origem_tracker`` — desempate
          determinístico em todas as réplicas, independente da ordem de
          chegada;
        * ``ativo=False`` vira tombstone; ``ativo=True`` sobre tombstone
          mais antigo remove o tombstone e registra a fonte.

        A tabela ``nome_peer_to_endereco`` NÃO é tocada: presença e
        failure detection (``last_seed_ts``) são responsabilidade do
        tracker ao qual o peer reporta SEED_REPORT; a própria entry
        carrega ip/porta, suficiente para responder buscas.

        Returns:
            ``True`` se a entrada foi aplicada, ``False`` se descartada
            pelo LWW.
        """
        with self._lock:
            versao_local = self._versao_local_locked(entry.hash, entry.nome_peer)
            if versao_local is not None and (timestamp, origem_tracker) <= versao_local:
                return False  # LWW: desatualizada (empate vence maior tracker_id)
            if entry.ativo:
                self._aplicar_registro_remoto_locked(entry, origem_tracker, timestamp)
            else:
                self._aplicar_tombstone_remoto_locked(entry, origem_tracker, timestamp)
            return True

    def apply_full_sync(self, entries: list[FullSyncEntry]) -> int:
        """Aplica um ``FULL_SYNC`` inteiro via LWW — reconciliação anti-entropy.

        Cada peer de cada entrada vira uma escrita LWW independente, usando o
        ``timestamp`` e a ``origem`` que viajam por peer (não no nível da
        mensagem, ao contrário do ``SYNC_TABLE``). Reaproveita
        :meth:`apply_sync_entry`, então herda todas as regras de desempate.

        É idempotente: reaplicar o mesmo estado não muda nada (cada entrada
        empata com a local e é descartada). Essa é a propriedade que torna o
        anti-entropy periódico seguro — main.tex §"Reconciliação anti-entropy".

        Returns:
            Quantas entradas ``(hash, peer)`` foram efetivamente aplicadas.
        """
        aplicadas = 0
        for entry in entries:
            for peer in entry.peers:
                convertida = SyncTableEntry(
                    hash=entry.hash,
                    nome_peer=peer.nome_peer,
                    ip=peer.ip,
                    porta=peer.porta,
                    ativo=peer.ativo,
                    nome=entry.nome if peer.ativo else None,
                    tamanho=entry.tamanho if peer.ativo else None,
                    n_chunks=entry.n_chunks if peer.ativo else None,
                )
                if self.apply_sync_entry(convertida, peer.origem, peer.timestamp):
                    aplicadas += 1
        return aplicadas

    def expire_tombstones(self, retention_seconds: float) -> int:
        """Descarta tombstones mais velhos que ``retention_seconds`` (§6.2).

        Chamado periodicamente pela thread de ``src.tracker.tombstone``.

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

    def _registrar_fonte_locked(
        self,
        hash_arquivo: str,
        nome_peer: str,
        endereco: PeerAddress,
        *,
        refresh: bool,
    ) -> None:
        """Adiciona o peer como fonte; ``refresh=False`` preserva o timestamp.

        REGISTER_FILE explícito usa ``refresh=True`` (evento novo para LWW);
        o anti-entropy do SEED_REPORT usa ``refresh=False`` para não gerar
        churn de timestamp a cada 3 minutos.
        """
        fontes = self.hash_to_peers.setdefault(hash_arquivo, {})
        if not refresh and nome_peer in fontes:
            return
        fontes[nome_peer] = PeerEntry(
            nome_peer=nome_peer,
            ip=endereco.ip,
            porta=endereco.porta,
            ativo=True,
            timestamp=self._clock(),
            origem=self._tracker_id,
        )
        self._descartar_tombstone_locked(hash_arquivo, nome_peer)

    def _tombstone_locked(self, hash_arquivo: str, nome_peer: str) -> None:
        entry = self.hash_to_peers.get(hash_arquivo, {}).pop(nome_peer, None)
        if entry is None:
            raise NotFoundError(
                f"peer {nome_peer!r} não é fonte do hash {hash_arquivo!r}"
            )
        self.tombstones.setdefault(hash_arquivo, {})[nome_peer] = TombstoneEntry(
            nome_peer=nome_peer,
            ip=entry.ip,
            porta=entry.porta,
            timestamp=self._clock(),
            origem=self._tracker_id,
        )

    def _atualizar_fontes_locked(self, nome_peer: str, ip: str, porta: int) -> None:
        agora = self._clock()
        for fontes in self.hash_to_peers.values():
            entry = fontes.get(nome_peer)
            if entry is None:
                continue
            entry.ip = ip
            entry.porta = porta
            entry.timestamp = agora
            entry.origem = self._tracker_id

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
        """Versão LWW local de (hash, peer): ``(timestamp, origem)`` ou ``None``.

        A versão vigente está em ``hash_to_peers`` (fonte ativa) ou em
        ``tombstones`` (remoção) — nunca em ambos.
        """
        fonte = self.hash_to_peers.get(hash_arquivo, {}).get(nome_peer)
        if fonte is not None:
            return (fonte.timestamp, fonte.origem)
        tombstone = self.tombstones.get(hash_arquivo, {}).get(nome_peer)
        if tombstone is not None:
            return (tombstone.timestamp, tombstone.origem)
        return None

    def _aplicar_registro_remoto_locked(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float
    ) -> None:
        if entry.hash not in self.hash_to_metadata and entry.nome is not None:
            # Metadados viajam no SYNC_TABLE (extensão do Listing 7.2) para
            # que este tracker responda buscas por nome sem SEARCH_FORWARD.
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
        )

    def _aplicar_tombstone_remoto_locked(
        self, entry: SyncTableEntry, origem_tracker: str, timestamp: float
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
        )

    def _peers_ativos_locked(self, hash_arquivo: str) -> list[SearchResultPeer]:
        return [
            SearchResultPeer(nome_peer=e.nome_peer, ip=e.ip, porta=e.porta)
            for e in self.hash_to_peers.get(hash_arquivo, {}).values()
            if e.ativo
        ]
