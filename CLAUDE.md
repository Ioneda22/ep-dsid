# CLAUDE.md — PeerSpot

> Guia prescritivo para o Claude Code implementar o sistema distribuído **PeerSpot**.
> Este documento contém **todas as decisões já tomadas**. Sua tarefa é implementá-las fielmente.

---

## 1. Visão Geral

**PeerSpot** é uma rede P2P não estruturada hierárquica para compartilhamento de arquivos de áudio, com **super peers (trackers)** que mantêm índices replicados.

- **Peers**: armazenam, compartilham e baixam arquivos em chunks.
- **Trackers**: mantêm índice global replicado via flooding TCP, organizam clusters de peers.
- **Bootstrap node**: tracker designado estaticamente para reintegração de trackers caídos.

**Documento de referência:** `main.tex` (especificação completa do projeto). Em caso de conflito entre este `CLAUDE.md` e o `main.tex`, o `main.tex` prevalece — peça esclarecimento antes de divergir.

---

## 2. Stack Tecnológica (Obrigatória)

| Aspecto | Escolha | Justificativa |
|---|---|---|
| Linguagem | Python **3.11+** | Type hints modernos, `match/case`, performance |
| API peer↔tracker | **FastAPI** + Uvicorn | REST sobre HTTP, async nativo |
| Tracker↔Tracker | **sockets TCP brutos** (`socket` + `threading`) | Conforme Listing do `main.tex` (Seção 8) |
| Peer↔Peer | **sockets TCP brutos** | Header JSON + payload binário |
| Persistência | **SQLite** (`sqlite3` stdlib) | Apenas para dados duráveis do tracker (usuários, playlists) |
| Índice em memória | `dict` + `threading.Lock()` | Tabelas `nome→hash`, `hash→peers`, `nome_peer→endereço` |
| Hashing | `hashlib.sha256` | Identificador de arquivos |
| Mensagens do protocolo | **pydantic v2** (`BaseModel`) | Schema + validação das 19 mensagens em `src/common/messages.py`, reusadas como corpos das rotas FastAPI |
| Serialização | `json` (stdlib) | Mensagens de controle |
| Logging | `logging` (stdlib) | Configuração centralizada |
| Testes | **pytest** + `pytest-asyncio` + `httpx` (cliente HTTP dos testes) | Unitários e integração |
| Execução multi-nó | **Múltiplos terminais** (processos Python em `127.0.0.1`, portas distintas) | Simula a rede distribuída sem virtualização — conforme `main.tex` Seção 5.3 |
| Configuração | **YAML** (`pyyaml`) ou variáveis de ambiente | Arquivos `config/*.yaml` |
| CLI | `input()` / `print()` puros | **Sem TUI ou GUI** — foco no sistema distribuído |

> **Sem Docker / sem contêineres.** A simulação de múltiplos peers e trackers é feita abrindo um terminal por nó, cada processo escutando em `127.0.0.1` numa porta distinta. Não crie `Dockerfile`, `docker-compose.yml` nem qualquer dependência de contêiner.

**Não use** bibliotecas além das listadas sem perguntar primeiro.

---

## 3. Estrutura de Diretórios (Obrigatória)

```
peerspot/
├── CLAUDE.md                       # este arquivo
├── main.tex                        # especificação
├── README.md                       # como rodar (gerar ao final)
├── requirements.txt
│
├── scripts/                        # conveniência para subir nós em terminais
│   ├── run_tracker.sh              # wrapper: python -m src.tracker.main --config ...
│   ├── run_peer.sh                 # wrapper: python -m src.peer.main --config ...
│   └── run_all_trackers.sh         # abre os 3 trackers (tmux/terminais), opcional
│
├── config/
│   ├── tracker-1.yaml
│   ├── tracker-2.yaml
│   ├── tracker-3.yaml
│   ├── peer-alice.yaml
│   ├── peer-bob.yaml
│   └── peer-carol.yaml
│
├── src/
│   ├── __init__.py
│   │
│   ├── common/                     # código compartilhado entre peer e tracker
│   │   ├── __init__.py
│   │   ├── messages.py             # modelos pydantic (BaseModel) de TODAS as mensagens (Listing 7.2 do main.tex)
│   │   ├── protocol.py             # helpers de envio/recepção TCP (framing por \n, JSON, header+payload)
│   │   ├── hashing.py              # SHA-256 de arquivos e chunks
│   │   ├── chunking.py             # split/join de arquivos em chunks
│   │   ├── logging_config.py       # setup de logging padronizado
│   │   ├── config.py               # parsing de YAML/env vars
│   │   └── errors.py               # exceções customizadas e códigos de ERROR
│   │
│   ├── tracker/
│   │   ├── __init__.py
│   │   ├── main.py                 # entrypoint: python -m src.tracker.main --config ...
│   │   ├── api.py                  # FastAPI app: rotas REST para peers
│   │   ├── index.py                # estado em memória: nome→hash, hash→peers, nome_peer→endereço
│   │   ├── sync_server.py          # servidor TCP: SYNC_TABLE / SYNC_PULL / SYNC_DIGEST / SEARCH_FORWARD / TRACKER_REJOIN
│   │   ├── sync_client.py          # outbound: flooding SYNC_TABLE/SYNC_DIGEST, reparo SYNC_PULL, reintegração
│   │   ├── routing.py              # SEARCH_FORWARD entre trackers (TTL, query_id)
│   │   ├── tombstone.py            # marcação e expiração de tombstones (10 min)
│   │   ├── anti_entropy.py         # digest de versões periódico (push SYNC_DIGEST, backstop do delta perdido)
│   │   ├── failure_detector.py     # timeout do SEED_REPORT (2 rodadas = 6 min → tombstone)
│   │   ├── rebalance.py            # lógica de REASSIGN_TRACKER após TRACKER_REJOIN
│   │   ├── persistence.py          # SQLite: usuários, playlists
│   │   └── handlers.py             # lógica de negócio chamada pela camada API
│   │
│   └── peer/
│       ├── __init__.py
│       ├── main.py                 # entrypoint: python -m src.peer.main --config ...
│       ├── cli.py                  # menu input()/print() — comandos: upload, search, download, list, quit
│       ├── tracker_client.py       # cliente REST para o tracker (com fallback à lista TRACKERS)
│       ├── tcp_server.py           # servidor TCP que atende CHUNK_LIST_REQUEST e CHUNK_REQUEST
│       ├── tcp_client.py           # cliente TCP para baixar chunks de outros peers
│       ├── chunk_manager.py        # controla quais chunks foram baixados; monta planos de download paralelo
│       ├── storage.py              # leitura/escrita de chunks no disco, indexados por hash
│       ├── seed_reporter.py        # thread que envia SEED_REPORT a cada 3 minutos
│       └── downloader.py           # orquestra busca → CHUNK_LIST_REQUEST → download paralelo → re-registro
│
├── data/                           # criado em runtime (gitignored)
│   ├── tracker-1/                  # SQLite DB
│   └── peer-alice/                 # chunks armazenados, indexados por hash
│
├── logs/                           # criado em runtime (gitignored)
│
└── tests/
    ├── __init__.py
    ├── unit/
    │   ├── test_messages.py
    │   ├── test_hashing.py
    │   ├── test_chunking.py
    │   ├── test_index.py
    │   ├── test_tombstone.py
    │   └── test_lww.py
    └── integration/
        ├── test_register_and_search.py
        ├── test_sync_flooding.py
        ├── test_download_parallel.py
        ├── test_tracker_rejoin.py
        └── test_failure_detection.py
```

---

## 4. Convenções de Código (Obrigatórias)

1. **Type hints em tudo.** Sem exceções. Use `from __future__ import annotations`.
2. **Docstrings** em todos os módulos públicos, classes e funções não-triviais. Estilo Google.
3. **Logging em vez de `print`**, exceto na CLI do peer (`src/peer/cli.py`).
4. **Nomes em português** para conceitos do domínio (`nome_peer`, `nome_musica`, `tracker_id`) **conforme já definido no `main.tex` Listing 7.2**. Nomes técnicos em inglês (`socket`, `thread`, `handler`).
5. **`threading.Lock()`** protegendo TODA estrutura de índice compartilhada. Nunca acesse `index` sem o lock.
6. **Sem `print` de stack traces** — use `logger.exception()`.
7. **Sem mutáveis como default arguments.**
8. **Quebra de mensagens TCP por `\n`**: toda mensagem JSON termina em `\n`; o receptor lê até `\n`.
9. **Transferência de chunk** = header JSON (com `\n`) **seguido imediatamente** do payload binário de exatamente `payload_bytes` bytes na mesma conexão.
10. **NTP**: assumimos relógio sincronizado. Use `time.time()` para timestamps. Não tente sincronizar manualmente.

---

## 5. Mensagens do Protocolo

**Todas as mensagens estão definidas no `main.tex` Listing 7.2 (Seção 7.2).** Implemente-as **literalmente** em `src/common/messages.py` como modelos **pydantic** (`BaseModel`). Lista completa:

### Entrada e presença (peer → tracker)
- `PEER_HELLO` — apresentação inicial
- `PEER_LEAVE` — saída ordenada
- `UPDATE_IP` — notificação de mudança de IP
- `SEED_REPORT` — relatório periódico de hashes (a cada **3 minutos**)

### Registro e busca
- `REGISTER_FILE` (peer → tracker) — registra upload ou re-registro após download
- `SEARCH_FILE` (peer → tracker) — busca com `query_id` UUID e `ttl=3` inicial
- `SEARCH_FORWARD` (tracker → tracker) — roteamento entre trackers
- `SEARCH_RESULT` (tracker → peer ou tracker → tracker)

### Transferência (peer ↔ peer)
- `CHUNK_LIST_REQUEST` / `CHUNK_LIST`
- `CHUNK_REQUEST` / `CHUNK_DATA_HEADER` (+ payload binário)

### Remoção
- `PEER_LEAVE_FILE` (peer → tracker)

### Sincronização (tracker → tracker, TCP unicast)
- `SYNC_TABLE` — atualização incremental com `origem`, `seq`, `timestamp`, `entries` (cada entry com `ativo: bool` distinguindo adição de tombstone). O par `(origem, seq)` identifica a escrita; o receptor mantém um **vetor de versões** `visto[origem]` (maior `seq` aplicado por origem) e um **conjunto de pendências** `(origem, desde_seq)` para as lacunas em aberto. O `seq` só **detecta** perda — o desempate de conflito continua sendo o LWW por `timestamp`.
- `SYNC_DIGEST` — vetor de versões (`versoes: {tracker_id: seq}`), enviado periodicamente; quem recebe compara componente a componente e pede o que falta.
- `SYNC_PULL` — reparo direcionado: `faltando: [{origem, desde_seq}]`, onde `desde_seq` é o `visto[origem]` capturado no instante da lacuna (guardado na pendência). A resposta são uma ou mais `SYNC_TABLE` (um evento por `seq`) na **mesma conexão TCP**, incluindo tombstones. `desde_seq=0` pede o estado inteiro de uma origem (reconstrução na reintegração).

### Gestão de membros (trackers)
- `TRACKER_REJOIN` — tracker que volta se anuncia ao primeiro conhecido que aceitar a conexão
- `TRACKER_LIST` — resposta ao `TRACKER_REJOIN`: só a membership (trackers ativos), **sem** índice
- `TRACKER_ANNOUNCE` — propaga o novo tracker aos demais
- `REASSIGN_TRACKER` (tracker → peer) — informa novo tracker ao peer durante rebalance

### Erro genérico
- `ERROR` com `ref_type`, `ref_id`, `code`, `mensagem`

Códigos de `ERROR` padronizados (defina em `src/common/errors.py`):
`NOT_FOUND`, `INVALID_HASH`, `INVALID_CHUNK`, `UNAUTHORIZED`, `PEER_UNKNOWN`, `TRACKER_UNKNOWN`, `MALFORMED_MESSAGE`, `INTERNAL_ERROR`.

---

## 6. Arquitetura do Tracker — Especificação Detalhada

### 6.1 Camadas (arquitetura em camadas, `main.tex` Seção 4.1)

1. **API REST (FastAPI)** — `src/tracker/api.py`
   - `POST /peers/hello` → `PEER_HELLO`
   - `POST /peers/leave` → `PEER_LEAVE`
   - `POST /peers/update-ip` → `UPDATE_IP`
   - `POST /peers/seed-report` → `SEED_REPORT`
   - `POST /files/register` → `REGISTER_FILE`
   - `POST /files/leave` → `PEER_LEAVE_FILE`
   - `POST /search` → `SEARCH_FILE`
   - `GET /trackers` → lista de trackers conhecidos (espelha `trackers_conhecidos` do `TRACKER_LIST`)
   - `GET /health` → healthcheck

2. **Lógica de negócio** — `src/tracker/handlers.py`
   - Funções puras (na medida do possível) chamadas pela camada API.

3. **Persistência** — `src/tracker/persistence.py`
   - SQLite com tabelas: `usuarios(nome_peer PRIMARY KEY, criado_em)`, `playlists(id, dono, nome, criada_em)`, `playlist_itens(playlist_id, hash, ordem)`.

4. **Sincronização** — `src/tracker/sync_server.py` + `sync_client.py`
   - Servidor TCP separado em porta dedicada (default **9001**).
   - Cliente faz flooding TCP unicast paralelo (uma thread por destino) do `SYNC_TABLE` (com `seq`) e do `SYNC_DIGEST`.
   - **Detecção por `seq` + reparo direcionado**: cada escrita local incrementa `meu_seq` e carimba a(s) entrada(s) com `(origem, seq)`; cada tracker mantém `visto[origem]` (vetor de versões). Um `SYNC_TABLE` com `seq > visto[origem]+1` revela uma lacuna → abre uma **pendência** (guardando o `desde_seq` capturado, o menor por origem) e dispara `SYNC_PULL({origem, desde_seq})`. As escritas fora de ordem são aplicadas na hora e `visto` avança por `max`; o reparo é idempotente (o LWW no receptor descarta o velho) e a pendência fecha quando a resposta chega.
   - **Digest periódico** (`src/tracker/anti_entropy.py`): push de `SYNC_DIGEST` (só o `visto`) a cada `digest_interval_seconds` (default **300s = 5 min**, < `tombstone_retention_seconds`), cobrindo o ponto cego da detecção inline (a última escrita de um tracker se perde e ele fica em silêncio).

### 6.2 Índice em memória — `src/tracker/index.py`

```python
class Index:
    # Estado protegido por self._lock = threading.Lock()
    nome_to_hashes: dict[str, set[str]]                       # "Imagine" -> {hash1, hash2}
    hash_to_metadata: dict[str, FileMetadata]                 # hash -> (nome, tamanho, n_chunks)
    hash_to_peers: dict[str, dict[str, PeerEntry]]            # hash -> {nome_peer -> PeerEntry}
    nome_peer_to_endereco: dict[str, PeerAddress]             # nome_peer -> (ip, porta, last_seed_ts)
    tombstones: dict[str, dict[str, TombstoneEntry]]          # hash -> {nome_peer -> tombstone com timestamp}
```

- `PeerEntry`: `nome_peer, ip, porta, ativo: bool, timestamp: float`.
- Todo método público adquire `self._lock`.
- `apply_sync_entry(entry, origem_tracker)` implementa **LWW**:
  - Compara `entry.timestamp` com o local. Maior vence. Empate → maior `tracker_id` vence.
  - Se `entry.ativo == False`, vira tombstone.
- `expire_tombstones()` roda em thread dedicada a cada 60s, remove tombstones com idade > **10 minutos**.

### 6.3 Failure detector — `src/tracker/failure_detector.py`

- Thread roda a cada 60s.
- Para cada `nome_peer`, calcula `time.time() - last_seed_ts`.
- Se > **6 minutos** (2 rodadas de `SEED_REPORT`): marca **todas** as entradas desse peer como tombstone, propaga via `SYNC_TABLE`.

### 6.4 Roteamento de buscas — `src/tracker/routing.py`

- Mantém cache `query_id → set[tracker_id_já_consultado]` para evitar laços.
- `SEARCH_FILE` recebido sem hit local:
  - Se `ttl > 0`: envia `SEARCH_FORWARD(ttl-1)` aos trackers ainda não consultados.
  - Coleta `SEARCH_RESULT` por **assíncrono com timeout de 2s**.
- Se resultado vier após o timeout, descarta.
- Resposta ao peer carrega `resultados=[]` se nada for encontrado dentro do timeout.

### 6.5 Reintegração e rebalance — `src/tracker/rebalance.py`

- O tracker que volta se anuncia com `TRACKER_REJOIN` ao **primeiro conhecido reachable** (o bootstrap não é um nó fixo — é o primeiro da lista que aceita a conexão).
- Quem recebe `TRACKER_REJOIN` responde com `TRACKER_LIST` (só a membership, **sem** índice) e propaga `TRACKER_ANNOUNCE` aos demais.
- De posse da membership, o tracker **reconstrói o índice** como caso particular do reparo: um `SYNC_PULL(desde_seq=0)` por origem conhecida (inclusive a própria), recebendo o estado atual (com tombstones) como `SYNC_TABLE`. Ao aplicar por LWW, **inicializa `visto`** e **restaura `meu_seq` = maior `seq` das entradas com `origem == meu_id`**, evitando reuso de `seq` após reinício sem persistir em disco.
- Depois, cada tracker ativo cede `floor(meus_peers / N_trackers)` peers ao reintegrado via `REASSIGN_TRACKER` (rebalance — Fase 5).

### 6.6 Configuração do tracker (YAML)

> **Execução local:** todos os nós rodam em `127.0.0.1`. Como compartilham a mesma máquina, **cada tracker usa um par de portas distinto** (`api_port` e `sync_port`). Paths são **relativos** à raiz do projeto (não use paths absolutos como `/data`).

```yaml
# config/tracker-1.yaml
tracker_id: tracker-1
ip: 127.0.0.1
api_port: 8001          # FastAPI (REST com peers)   — tracker-2: 8002, tracker-3: 8003
sync_port: 9001         # TCP (sincronização)        — tracker-2: 9002, tracker-3: 9003
is_bootstrap: true      # apenas tracker-1 é bootstrap
known_trackers:
  - { tracker_id: tracker-2, ip: 127.0.0.1, sync_port: 9002 }
  - { tracker_id: tracker-3, ip: 127.0.0.1, sync_port: 9003 }
db_path: data/tracker-1/peerspot.db
log_path: logs/tracker-1.log
log_level: INFO

# Constantes operacionais
seed_report_timeout_seconds: 360       # 6 min = 2 rodadas perdidas
tombstone_retention_seconds: 600       # 10 min
sync_outbound_timeout_seconds: 3
search_forward_timeout_seconds: 2
digest_interval_seconds: 300           # 5 min; push SYNC_DIGEST entre trackers (< retenção do tombstone)
```

---

## 7. Arquitetura do Peer — Especificação Detalhada

### 7.1 Threads internas

| Thread | Responsabilidade |
|---|---|
| **Main** | CLI (`input()`/`print()`) — `src/peer/cli.py` |
| **TCP server** | aceita conexões de outros peers (`CHUNK_LIST_REQUEST`, `CHUNK_REQUEST`) |
| **Seed reporter** | envia `SEED_REPORT` a cada 3 min |
| **Download workers** | pool dinâmico de threads para baixar chunks em paralelo |

### 7.2 CLI — comandos obrigatórios

```
> help                    # lista comandos
> upload <caminho>        # faz upload de um arquivo de áudio
> search <nome>           # busca por nome legível
> download <hash>         # baixa um arquivo pelo hash
> list                    # lista arquivos disponíveis localmente
> remove <hash>           # remove um arquivo local e notifica tracker
> peers <hash>            # lista peers que possuem um hash
> playlist create <nome>  # cria playlist
> playlist add <playlist> <hash>
> playlist show <playlist>
> status                  # mostra tracker atual, peers conhecidos, downloads em andamento
> quit                    # sai ordenadamente (envia PEER_LEAVE)
```

### 7.3 Chunking

- **Tamanho de chunk:** **256 KB** (262144 bytes).
- `n_chunks = ceil(tamanho_total / 262144)`.
- Cada chunk armazenado em `data/<peer_nome>/<hash>/chunk_<index>.bin`.
- Hash do arquivo completo = SHA-256 do conteúdo concatenado (não dos chunks individuais).

### 7.4 Download paralelo — `src/peer/downloader.py`

Pseudocódigo do plano de download:

```
1. SEARCH_FILE → recebe lista de peers fonte.
2. Para cada peer fonte (em paralelo): CHUNK_LIST_REQUEST → CHUNK_LIST.
3. Monta dicionário: chunk_index -> [peers que têm].
4. Para cada chunk faltante:
   - Escolhe a fonte com MENOR número de chunks já atribuídos
     (balanceamento rarest-first simplificado).
   - Despacha CHUNK_REQUEST em uma thread do pool.
5. Cada CHUNK_DATA recebido: valida tamanho == payload_bytes, grava em disco.
6. Ao completar todos os chunks:
   - Concatena, valida SHA-256 do arquivo final == hash esperado.
   - Se inválido: descarta, falha o download, loga erro.
   - Se válido: envia REGISTER_FILE ao tracker (re-registro).
```

- **Timeout por CHUNK_REQUEST:** 10s. Em falha, tenta outra fonte; se nenhuma, falha o download.
- **Sem retransmissão automática** se todas as fontes falharem — reporta erro ao usuário.

### 7.5 Fallback de tracker

- Lista `TRACKERS` carregada do YAML (Listing `lst:fallback` do `main.tex`, seção "Tolerância a Falhas de Tracker").
- `tracker_client.py` tenta o primeiro; em `timeout/ConnectionRefused`, passa para o próximo.
- Após sucesso, atualiza `current_tracker_index` (não reseta para 0 em cada chamada).
- Ao receber `REASSIGN_TRACKER`: troca para o novo tracker e envia `PEER_HELLO` lá.

### 7.6 Configuração do peer (YAML)

> **Execução local:** cada peer usa uma `porta` distinta para seu servidor TCP (alice: 7001, bob: 7002, carol: 7003). A lista `trackers` aponta para as `api_port` dos trackers em `127.0.0.1`.

```yaml
# config/peer-alice.yaml
nome_peer: alice
ip: 127.0.0.1
porta: 7001                # TCP server para chunks (peer-peer) — bob: 7002, carol: 7003
storage_dir: data/peer-alice
log_path: logs/peer-alice.log
log_level: INFO
trackers:
  - { tracker_id: tracker-1, ip: 127.0.0.1, api_port: 8001 }
  - { tracker_id: tracker-2, ip: 127.0.0.1, api_port: 8002 }
  - { tracker_id: tracker-3, ip: 127.0.0.1, api_port: 8003 }

# Constantes operacionais
chunk_size_bytes: 262144           # 256 KB
seed_report_interval_seconds: 180  # 3 min
chunk_request_timeout_seconds: 10
download_pool_size: 8
```

---

## 8. Execução Local via Múltiplos Terminais

**Sem Docker.** A rede distribuída é simulada abrindo **um terminal por nó**, cada processo Python escutando em `127.0.0.1` numa porta distinta (definida no YAML). O cenário padrão de teste é **3 trackers + 3 peers**, ou seja, 6 terminais.

### 8.1 Ordem de inicialização

1. **Primeiro o bootstrap node** (`tracker-1`), pois os demais trackers enviam `TRACKER_REJOIN` a ele ao subir.
2. **Depois `tracker-2` e `tracker-3`** — cada um, ao iniciar, envia `TRACKER_REJOIN` ao primeiro conhecido reachable, recebe `TRACKER_LIST` e reconstrói o índice via `SYNC_PULL(desde_seq=0)`.
3. **Por fim os peers** (`alice`, `bob`, `carol`) — cada um envia `PEER_HELLO` ao primeiro tracker da sua lista.

### 8.2 Comandos por terminal

```bash
# Terminal 1 — bootstrap
python -m src.tracker.main --config config/tracker-1.yaml

# Terminal 2
python -m src.tracker.main --config config/tracker-2.yaml

# Terminal 3
python -m src.tracker.main --config config/tracker-3.yaml

# Terminal 4 — peer com CLI interativa
python -m src.peer.main --config config/peer-alice.yaml

# Terminal 5
python -m src.peer.main --config config/peer-bob.yaml

# Terminal 6
python -m src.peer.main --config config/peer-carol.yaml
```

### 8.3 Scripts de conveniência (`scripts/`)

- `run_tracker.sh <n>` → atalho para `python -m src.tracker.main --config config/tracker-<n>.yaml`
- `run_peer.sh <nome>` → atalho para `python -m src.peer.main --config config/peer-<nome>.yaml`
- `run_all_trackers.sh` (opcional) → usa `tmux` ou abre terminais para subir os 3 trackers de uma vez. Mantenha simples; se `tmux` não estiver disponível, o script apenas imprime os comandos a rodar manualmente.

Os scripts devem: ativar o virtualenv se existir (`.venv/bin/activate`), exportar `PYTHONPATH=.` e então invocar o módulo. São conveniências — toda a funcionalidade deve ser acessível rodando os comandos `python -m ...` diretamente.

### 8.4 Simulação de falhas

- **Derrubar um nó:** `Ctrl+C` no terminal correspondente (crash failure controlado).
- **Tracker volta:** reabrir o terminal e rodar o mesmo comando → dispara `TRACKER_REJOIN` automático, recebe `TRACKER_LIST` e reconstrói o índice via `SYNC_PULL(desde_seq=0)`.
- **Peer morre abruptamente:** fechar o terminal sem usar `quit` → após `seed_report_timeout_seconds` (6 min) o tracker o marca como tombstone.

### 8.5 Diretórios de runtime

`data/` e `logs/` são criados automaticamente na primeira execução (paths relativos do YAML, a partir da raiz do projeto). Ambos estão no `.gitignore`. Para um "reset limpo" do ambiente, basta apagar `data/` e `logs/`.

---

## 9. Plano de Implementação (Ordem Obrigatória)

Implemente nesta ordem. **Não avance** para a próxima fase sem que a anterior tenha testes passando.

### Fase 1 — Fundação
1. `src/common/messages.py` — todas as mensagens como modelos pydantic
2. `src/common/hashing.py` — SHA-256 de arquivos e bytes
3. `src/common/chunking.py` — split/join
4. `src/common/protocol.py` — `send_json_line()`, `recv_json_line()`, `send_chunk()`, `recv_chunk()`
5. `src/common/logging_config.py`, `src/common/config.py`, `src/common/errors.py`
6. **Testes unitários** de tudo acima.

### Fase 2 — Tracker básico (1 tracker, sem sync)
1. `src/tracker/index.py` com lock
2. `src/tracker/handlers.py`: `PEER_HELLO`, `REGISTER_FILE`, `SEARCH_FILE` (sem forward), `SEED_REPORT`
3. `src/tracker/api.py` — rotas FastAPI
4. `src/tracker/persistence.py` — SQLite mínimo (usuários)
5. `src/tracker/main.py`
6. **Teste integração**: 1 peer fictício registra arquivo, outro busca, encontra.

### Fase 3 — Peer básico (1 peer)
1. `src/peer/storage.py`
2. `src/peer/tracker_client.py` (sem fallback ainda)
3. `src/peer/tcp_server.py` (atende `CHUNK_LIST_REQUEST`, `CHUNK_REQUEST`)
4. `src/peer/tcp_client.py`
5. `src/peer/chunk_manager.py`
6. `src/peer/downloader.py` (download sequencial primeiro)
7. `src/peer/cli.py` com comandos `upload`, `search`, `download`, `list`, `quit`
8. `src/peer/main.py`
9. **Teste integração**: dois peers + um tracker, upload em A, download em B, SHA-256 confere.

### Fase 4 — Sincronização entre trackers
1. `src/tracker/sync_server.py` — servidor TCP
2. `src/tracker/sync_client.py` — flooding paralelo
3. `src/tracker/index.py` — `apply_sync_entry()` com LWW
4. `src/tracker/tombstone.py`
5. `src/tracker/routing.py` — `SEARCH_FORWARD` com TTL
6. **Teste integração**: 3 trackers, upload no tracker-1, busca no tracker-3 encontra.
7. **Teste integração**: dois uploads concorrentes, ambos aparecem em todos os trackers.

### Fase 5 — Robustez e dinamismo
1. `src/peer/seed_reporter.py` (3 min)
2. `src/tracker/failure_detector.py` (timeout 6 min)
3. `src/peer/tracker_client.py` — fallback completo
4. Download **paralelo** no peer (substitui o sequencial da Fase 3)
5. `TRACKER_REJOIN` → `TRACKER_LIST` → `SYNC_PULL(desde_seq=0)`, `TRACKER_ANNOUNCE`, `SYNC_DIGEST`
6. `src/tracker/rebalance.py` + `REASSIGN_TRACKER`
7. **Teste integração**: matar tracker, peer faz fallback, retornar tracker, ele reconstrói o índice via `SYNC_PULL(desde_seq=0)`.

### Fase 6 — Funcionalidades de produto
1. Playlists (CRUD via API e CLI)
2. Comando `status` na CLI
3. Comando `peers <hash>`

### Fase 7 — Empacotamento de execução e documentação
1. `scripts/run_tracker.sh`, `scripts/run_peer.sh` e (opcional) `scripts/run_all_trackers.sh` conforme §8.3
2. Conjunto completo de configs: `config/tracker-{1,2,3}.yaml` e `config/peer-{alice,bob,carol}.yaml` com portas distintas (§6.6 e §7.6)
3. `README.md` com:
   - Pré-requisitos (Python 3.11+, `pip install -r requirements.txt`, virtualenv)
   - Como subir o ambiente em múltiplos terminais (§8.2), na ordem correta (bootstrap primeiro)
   - Como usar a CLI de um peer
   - Roteiro de demonstração (upload em alice, download em bob, derrubar tracker via Ctrl+C, etc.)
   - Como rodar os testes (`pytest -v`)
   - Limitações conhecidas

---

## 10. Testes — Diretrizes

- **Unitários**: cada módulo de `src/common/` e funções puras de `index.py`, `tombstone.py`, `chunking.py`.
- **Integração**: subprocessos reais escutando em portas locais (não mocks de socket). Use fixtures `pytest` que iniciam tracker em thread e finalizam ao fim do teste.
- **Determinismo**: para LWW, injete `time.time` via parâmetro (não monkeypatch global).
- **Sem rede externa**: todos os testes rodam em `127.0.0.1` com portas atribuídas dinamicamente.
- Comando único: `pytest -v` deve rodar tudo em < 60 segundos.

---

## 11. Restrições, Pegadinhas e Não-Fazeres

1. **NÃO** invente novas mensagens. Se algo parece faltar, releia o `main.tex` Listing 7.2; se ainda assim faltar, pergunte.
2. **NÃO** use `asyncio` no tracker para a sincronização TCP. O `main.tex` Listing 8.1 explicitamente usa `socket` + `threading`. Mantenha. FastAPI internamente é async — isso é ok.
3. **NÃO** misture o protocolo peer-peer com FastAPI. Peer-peer é socket TCP bruto.
4. **NÃO** persista o índice. O índice é em memória; ao reiniciar, o tracker se reconstrói via `SYNC_PULL(desde_seq=0)` na reintegração (e via `SEED_REPORT` dos peers ao longo do tempo).
5. **NÃO** implemente DHT, exclusão mútua distribuída entre trackers, ou eleição de líder. O `main.tex` explicitamente descarta essas abordagens (Seções 14 e 15).
6. **NÃO** use eventos `multiprocessing` ou IPC além de sockets. Todo paralelismo dentro de um nó é `threading`.
7. **NÃO** suprima exceções silenciosamente. Sempre logue com `logger.exception()` antes de tratar.
8. **NÃO** confie em ordem de chegada de `SYNC_TABLE`. LWW é a única autoridade.
9. **NÃO** crie um README até a Fase 7. Mantenha o foco em código + testes.
10. **PERGUNTE** antes de adicionar dependências fora da lista do §2.

---

## 12. Critérios de Aceitação Final

O sistema está pronto quando o seguinte roteiro funciona com os 6 nós rodando em terminais separados (§8.2):

1. ✅ 3 trackers e 3 peers sobem em 6 terminais (bootstrap primeiro) e se conectam.
2. ✅ `alice` faz upload de um MP3 de ~5 MB. `tracker-1` registra.
3. ✅ Em < 3s, `tracker-2` e `tracker-3` têm o hash no índice (verificável via `GET /search` em cada um).
4. ✅ `bob` (conectado ao `tracker-2`) busca pelo nome e encontra. Inicia download.
5. ✅ Download usa exclusivamente `alice` como fonte. Após conclusão, `bob` reaparece no índice como segunda fonte do hash.
6. ✅ `carol` (conectado ao `tracker-3`) baixa o mesmo arquivo agora distribuindo chunks entre `alice` e `bob`.
7. ✅ `Ctrl+C` no terminal do `tracker-1`. `alice` faz fallback para `tracker-2` automaticamente (próxima operação não-falha).
8. ✅ Reabrir o terminal e rodar `tracker-1` novamente. Tracker reintegra via `TRACKER_REJOIN`, recebe `TRACKER_LIST`, reconstrói o índice via `SYNC_PULL(desde_seq=0)` por origem, e alguns peers são reassignados a ele via `REASSIGN_TRACKER`.
9. ✅ `alice` remove o arquivo (`remove <hash>`). Em < 3s, `tracker-2` e `tracker-3` registram tombstone. `bob` continua aparecendo como fonte.
10. ✅ `pytest -v` passa 100% dos testes.

---

## 13. Em Caso de Dúvida

Se algo neste documento parece ambíguo ou conflitar com `main.tex`:
- **Releia a seção correspondente do `main.tex`** primeiro.
- Se persistir a dúvida, **pergunte ao usuário** antes de codar.
- Não invente comportamento "razoável" — o domínio é cheio de pegadinhas onde "razoável" diverge da especificação.

---

## 14. Diretrizes de Desenvolvimento Ágil e Boas Práticas

> Estas diretrizes valem para **todo** o código de `src/` e `tests/`. Complementam as Convenções da §4 — em caso de conflito pontual, a §4 (que reflete o `main.tex`) prevalece.

### 14.1 Estilo de Código

- **Funções: 4–20 linhas.** Acima disso, quebre em funções menores com nomes que revelem a intenção.
- **Arquivos: abaixo de 500 linhas.** Divida por responsabilidade (a estrutura da §3 já força isso — mantenha).
- **Uma coisa por função, uma responsabilidade por módulo (SRP).** O handler não faz I/O de socket; o socket não faz regra de negócio.
- **Nomes específicos e únicos.** Evite `data`, `handler`, `manager`, `process`, `info`. Prefira nomes que retornem poucos hits no `grep` (`apply_sync_entry`, `expire_tombstones`, `recv_chunk`).
- **Type hints explícitos em tudo** (já obrigatório na §4.1). Sem `Any`, sem `dict`/`list` crus quando há um modelo pydantic/`dataclass` (use os de `messages.py`). `from __future__ import annotations` no topo de cada módulo.
- **Sem duplicação.** Lógica compartilhada entre peer e tracker mora em `src/common/`. Se copiar-colar, extraia.
- **Early returns em vez de ifs aninhados.** Máximo de 2 níveis de indentação no corpo de uma função.
- **Mensagens de exceção incluem o valor ofensor e o formato esperado.** Use as exceções de `src/common/errors.py`; ex.: `raise InvalidHash(f"hash esperado sha256 de 64 hex, recebido {valor!r}")`.

### 14.2 Comentários

- **Preserve comentários existentes** em refatorações — eles carregam intenção e contexto.
- **Escreva o PORQUÊ, não o O QUÊ.** Pule `# incrementa i` antes de `i += 1`.
- **Docstrings (estilo Google) em funções públicas:** intenção + um exemplo de uso, conforme §4.2.
- **Referencie a origem** quando uma linha existe por causa de uma decisão específica: cite a seção do `main.tex` (ex.: `# LWW: empate vence maior tracker_id — main.tex §7.2`) ou o SHA do commit.

### 14.3 Testes

- **Roda com um único comando: `pytest -v`** (a suíte inteira em < 60s, §10).
- **Toda função nova ganha um teste.** Toda correção de bug ganha um teste de regressão que falha antes do fix.
- **Mocke I/O externo com fakes nomeados**, não stubs inline. Para sockets/trackers na integração, prefira **subprocessos/threads reais** em `127.0.0.1` com portas dinâmicas (§10) a mocks de socket.
- **Determinismo:** injete `time.time` por parâmetro (LWW, tombstones, failure detector) — nunca `monkeypatch` global (§10).
- **Testes F.I.R.S.T:** rápidos, independentes, repetíveis, auto-validáveis e escritos junto com o código.
- **Não avance de fase** (§9) sem os testes da fase anterior passando.

### 14.4 Dependências e Acoplamento

- **Injete dependências por construtor/parâmetro**, não via global ou import de estado mutável. Ex.: `Index`, conexão SQLite e config entram nos `handlers` por parâmetro — facilita o teste.
- **Nada de estado global mutável compartilhado** além do `Index` protegido por lock (§4.5).
- **Não adicione bibliotecas** fora da lista da §2 sem perguntar (§11.10).

### 14.5 Formatação

- **Use o formatador padrão da linguagem.** Rode `black src/ tests/` (e, se disponível, `ruff` para lint). Não discuta estilo além do que o formatador decide.
- Linhas, aspas e imports seguem o `black` — não os ajuste à mão.

### 14.6 Logging

- **`logging` estruturado para observabilidade/debug** (já obrigatório na §4.3); inclua `tracker_id`/`nome_peer`/`query_id` nos registros relevantes.
- **Texto puro (`print`) só na CLI do peer** (`src/peer/cli.py`), que é a única saída voltada ao usuário.
- **Nunca suprima exceção em silêncio:** `logger.exception()` antes de tratar (§11.7).
