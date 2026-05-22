# Changelog

Todas as mudanças notáveis do projeto PeerSpot são documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto segue [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Adicionado
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
