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
from src.common.messages import SearchResultEntry, SearchResultPeer, SyncTableEntry


@dataclass
class FileMetadata:
    """Metadados imutáveis de um arquivo registrado (do REGISTER_FILE inicial)."""

    nome: str
    tamanho: int
    n_chunks: int


@dataclass
class PeerEntry:
    """Um peer como fonte de um hash, com timestamp para LWW (§6.2)."""

    nome_peer: str
    ip: str
    porta: int
    ativo: bool
    timestamp: float


@dataclass
class PeerAddress:
    """Endereço corrente de um peer + instante do último SEED_REPORT."""

    ip: str
    porta: int
    last_seed_ts: float


@dataclass
class TombstoneEntry:
    """Remoção registrada de (hash, peer); expira após 10 min (Fase 4)."""

    nome_peer: str
    ip: str
    porta: int
    timestamp: float


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

    O relógio é injetado por construtor para testes determinísticos (§10):

        >>> indice = Index(clock=lambda: 1000.0)
        >>> indice.register_peer("alice", "127.0.0.1", 7001)
    """

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = threading.Lock()
        self._clock = clock
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
    ) -> None:
        """Aplica ``REGISTER_FILE``: upload original ou re-registro pós-download.

        No re-registro, ``nome``/``tamanho``/``n_chunks`` são opcionais — o
        tracker já os conhece do upload original (main.tex §7.2).

        Raises:
            PeerUnknownError: Se o peer não enviou ``PEER_HELLO`` antes.
            NotFoundError: Se o hash é desconhecido e os metadados faltam.
        """
        with self._lock:
            endereco = self._endereco_ou_erro_locked(nome_peer)
            self._garantir_metadata_locked(hash_arquivo, nome, tamanho, n_chunks)
            self._registrar_fonte_locked(hash_arquivo, nome_peer, endereco, refresh=True)

    def remove_peer_from_hash(self, hash_arquivo: str, nome_peer: str) -> None:
        """Aplica ``PEER_LEAVE_FILE``: o par (hash, peer) vira tombstone.

        Raises:
            NotFoundError: Se o peer não consta como fonte do hash.
        """
        with self._lock:
            self._tombstone_locked(hash_arquivo, nome_peer)

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
                    SearchResultEntry(hash=hash_arquivo, nome=meta.nome, peers=peers)
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

    def apply_sync_entry(self, entry: SyncTableEntry, origem_tracker: str) -> None:
        """Aplica uma entrada de ``SYNC_TABLE`` com resolução LWW.

        Stub: a sincronização entre trackers chega na Fase 4 (§9).
        """
        raise NotImplementedError(
            "apply_sync_entry será implementado na Fase 4 (sincronização entre trackers)"
        )

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
        )
        self.tombstones.get(hash_arquivo, {}).pop(nome_peer, None)

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

    def _peers_ativos_locked(self, hash_arquivo: str) -> list[SearchResultPeer]:
        return [
            SearchResultPeer(nome_peer=e.nome_peer, ip=e.ip, porta=e.porta)
            for e in self.hash_to_peers.get(hash_arquivo, {}).values()
            if e.ativo
        ]
