# Changelog

Todas as mudanças notáveis do projeto PeerSpot são documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto segue [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Alterado
- **Mensagens do protocolo migradas de `TypedDict` para pydantic v2**: os
  19 tipos do Listing 7.2 em `src/common/messages.py` agora são `BaseModel`
  com `type: Literal[...]` com default. `MESSAGE_MODELS` mapeia
  `type → modelo`; `REQUIRED_FIELDS` passa a ser derivado dos modelos; e
  `validate_message` valida via `model_validate` (além da presença, agora
  também checa o tipo dos campos), preservando a API (`ValueError`).
  `src/tracker/api.py` usa os modelos de `messages.py` diretamente como
  corpos das rotas — removidos os modelos `*Body` que os duplicavam;
  `handlers.py` e `index.py` usam acesso por atributo e constroem
  `SearchResult`/`SearchResultEntry`/`SearchResultPeer` como modelos.
  Documentação atualizada (`CLAUDE.md` §§2, 3, 5, 9 e 14.1 agora
  prescrevem pydantic em vez de TypedDict/dataclass para mensagens) e
  `pydantic>=2` explicitado em `requirements.txt`.

### Adicionado
- **Fase 2 — Tracker básico** (§9 do `CLAUDE.md`): um tracker, sem
  sincronização entre trackers (Fase 4), sem failure detector (Fase 5) e
  sem `SEARCH_FORWARD`:
  - `src/tracker/index.py` — classe `Index` (§6.2) com as tabelas
    `nome_to_hashes`, `hash_to_metadata`, `hash_to_peers`,
    `nome_peer_to_endereco` e `tombstones`, todas protegidas por um único
    `threading.Lock` (métodos `_locked` exigem o lock já adquirido).
    Métodos: `register_peer`, `remove_peer`, `update_peer_address`,
    `update_last_seed`, `register_file` (metadados opcionais no
    re-registro pós-download), `remove_peer_from_hash` (vira tombstone),
    `apply_seed_hashes` (anti-entropy do SEED_REPORT: hash omitido
    equivale a PEER_LEAVE_FILE, sem churn de timestamp para LWW),
    `search_by_name` (busca exata; hashes sem fonte ativa são omitidos),
    `get_peers_for_hash`, `get_snapshot` (cópia profunda) e
    `apply_sync_entry` como stub (`NotImplementedError`) até a Fase 4.
    Relógio injetável por construtor para testes determinísticos (§10).
  - `src/tracker/persistence.py` — SQLite (§6.1 camada 3): tabelas
    `usuarios`, `playlists`, `playlist_itens`; `init_db` (factory),
    `TrackerDB.registrar_usuario` (idempotente) e `listar_usuarios`;
    métodos de playlist como stubs até a Fase 6. Conexão serializada por
    lock (uvicorn despacha rotas síncronas em threadpool).
  - `src/tracker/handlers.py` — lógica de negócio (§6.1 camada 2) com
    dependências injetadas por parâmetro (§14.4): `handle_peer_hello`,
    `handle_peer_leave`, `handle_update_ip`, `handle_seed_report`
    (re-registra presença + anti-entropy), `handle_register_file`,
    `handle_peer_leave_file` e `handle_search_file` (local apenas;
    `resultados=[]` quando não há hit).
  - `src/tracker/api.py` — FastAPI (§6.1 camada 1): `POST /peers/hello`,
    `/peers/leave`, `/peers/update-ip`, `/peers/seed-report`,
    `/files/register`, `/files/leave`, `/search`; `GET /trackers` e
    `/health`. Corpos validados por modelos pydantic espelhando os
    TypedDicts do Listing 7.2. `PeerSpotError` vira mensagem `ERROR` do
    protocolo (404/400/403/500 conforme o código); payload malformado
    vira `ERROR` com `MALFORMED_MESSAGE` e HTTP 400. Rotas síncronas de
    propósito (threadpool do uvicorn + `threading.Lock` do índice).
  - `src/tracker/main.py` — entrypoint
    `python -m src.tracker.main --config config/tracker-1.yaml`: carrega
    YAML (`TrackerSettings`, incluindo constantes operacionais das fases
    futuras), configura logging, inicializa `Index` + SQLite e sobe o
    uvicorn. `sync_port` é lido mas ainda não usado (Fase 4).
  - `config/tracker-1.yaml` conforme §6.6.
  - Testes: `tests/unit/test_index.py` (23 testes, relógio fake
    injetado), `tests/unit/test_persistence.py` e
    `tests/integration/test_register_and_search.py` — uvicorn real em
    porta dinâmica de 127.0.0.1 (§10) exercitado via `httpx`: fluxo
    PEER_HELLO → REGISTER_FILE → SEARCH_FILE, PEER_LEAVE_FILE preserva as
    demais fontes, PEER_LEAVE remove o peer do índice, UPDATE_IP reflete
    na busca, SEED_REPORT reconcilia hashes, erros `PEER_UNKNOWN` e
    `MALFORMED_MESSAGE`. Suíte completa: 97 testes em ~6 s.
  - `httpx` adicionado a `requirements.txt` (e à tabela §2 do `CLAUDE.md`)
    como cliente HTTP dos testes de integração.
- **Fase 1 — Fundação** (§9 do `CLAUDE.md`): módulos compartilhados em
  `src/common/` totalmente implementados com type hints e docstrings:
  - `messages.py` — `TypedDict` de todas as 19 mensagens do Listing 7.2 do
    `main.tex` (PEER_HELLO, PEER_LEAVE, UPDATE_IP, SEED_REPORT,
    REGISTER_FILE, SEARCH_FILE, SEARCH_FORWARD, SEARCH_RESULT,
    CHUNK_LIST_REQUEST, CHUNK_LIST, CHUNK_REQUEST, CHUNK_DATA,
    PEER_LEAVE_FILE, SYNC_TABLE, FULL_SYNC, TRACKER_REJOIN,
    TRACKER_ANNOUNCE, REASSIGN_TRACKER, ERROR) + `validate_message`
    para checagem leve em runtime.
  - `hashing.py` — `sha256_bytes` e `sha256_file` (streaming em buffers
    de 1 MiB).
  - `chunking.py` — `split_file`, `join_chunks`, `chunk_count`; chunk
    default de 256 KiB (§7.3).
  - `protocol.py` — framing TCP por `\n` para JSON e `header+payload`
    para chunks. **Toda recepção passa por `MessageReader.recv_message`**
    (método único): mantém buffer interno entre chamadas tratando
    coalescimento TCP, e decide pela presença de `payload_bytes` no
    cabeçalho se a mensagem é JSON-pura (`payload=None`) ou se carrega
    payload binário (`payload=bytes`, possivelmente vazio quando
    `payload_bytes=0`). Não há `recv_json_line`/`recv_chunk` separados —
    obrigar o receptor a adivinhar o tipo enviado pelo emissor é um
    foot-gun cujo erro só aparece sob coalescimento. Funções de módulo
    expostas: `send_json_line`, `send_chunk`, `recv_exact` (primitiva).
  - `logging_config.py` — `setup_logging` com handlers de arquivo e
    `stderr`, idempotente.
  - `config.py` — `load_yaml` e `require_keys` com `ConfigError`.
  - `errors.py` — códigos `NOT_FOUND`, `INVALID_HASH`, `INVALID_CHUNK`,
    `UNAUTHORIZED`, `PEER_UNKNOWN`, `TRACKER_UNKNOWN`,
    `MALFORMED_MESSAGE`, `INTERNAL_ERROR`; hierarquia de exceções
    `PeerSpotError` + subclasses; `build_error_message`.
- Testes unitários em `tests/unit/` cobrindo todos os módulos acima
  (59 testes, 100% passando em < 1 s): `test_messages.py`, `test_hashing.py`,
  `test_chunking.py` (incl. arquivo cujo tamanho NÃO é múltiplo do chunk size),
  `test_protocol.py` (roundtrip via `socket.socketpair`, payload > 256 KiB,
  coalescimento TCP, conexão fechada, mistura JSON-pura/chunk no mesmo
  reader, travas arquiteturais contra reintrodução das APIs separadas),
  `test_errors.py`, `test_config.py`, `test_logging_config.py`.
- `pytest.ini` com `testpaths = tests`.
- Setup do projeto: estrutura de diretórios conforme §3 do `CLAUDE.md`
  (pacotes `src/common`, `src/tracker`, `src/peer` com módulos vazios, `tests/`,
  `config/`, `scripts/`, `data/`, `logs/`).
- `requirements.txt` com a stack obrigatória do §2 (FastAPI, Uvicorn, PyYAML,
  pytest, pytest-asyncio).
- `.gitignore` cobrindo `.venv/`, caches, `data/` e `logs/` de runtime.
- `README.md` com pré-requisitos e setup do ambiente.

### Removido
- Layout plano antigo (`common/`, `peer/`, `tracker/`), `pyproject.toml`,
  `peerspot.egg-info/`, `main.py` de exemplo, `config/trackers.json` e
  `scripts/lab.local.ps1`, substituídos pela estrutura do §3.
