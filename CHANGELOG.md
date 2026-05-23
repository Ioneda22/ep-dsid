# Changelog

Todas as mudanças notáveis do projeto PeerSpot são documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto segue [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Adicionado
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
    para chunks. **Toda recepção passa pela classe `MessageReader`**
    (única API de leitura): mantém buffer interno entre chamadas,
    tratando coalescimento TCP de mensagens em qualquer cenário
    (one-shot ou conexão persistente). Funções de módulo expostas:
    `send_json_line`, `send_chunk`, `recv_exact` (primitiva).
  - `logging_config.py` — `setup_logging` com handlers de arquivo e
    `stderr`, idempotente.
  - `config.py` — `load_yaml` e `require_keys` com `ConfigError`.
  - `errors.py` — códigos `NOT_FOUND`, `INVALID_HASH`, `INVALID_CHUNK`,
    `UNAUTHORIZED`, `PEER_UNKNOWN`, `TRACKER_UNKNOWN`,
    `MALFORMED_MESSAGE`, `INTERNAL_ERROR`; hierarquia de exceções
    `PeerSpotError` + subclasses; `build_error_message`.
- Testes unitários em `tests/unit/` cobrindo todos os módulos acima
  (50 testes, 100% passando em < 1 s): `test_messages.py`, `test_hashing.py`,
  `test_chunking.py` (incl. arquivo cujo tamanho NÃO é múltiplo do chunk size),
  `test_protocol.py` (roundtrip via `socket.socketpair`, payload > 256 KiB,
  coalescimento TCP, conexão fechada), `test_errors.py`, `test_config.py`,
  `test_logging_config.py`.
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
