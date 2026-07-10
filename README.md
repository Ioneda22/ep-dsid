# PeerSpot

Sistema distribuído **P2P não estruturado hierárquico** para compartilhamento de
arquivos de áudio, desenvolvido para a disciplina **Desenvolvimento de Sistemas de
Informação Distribuídos** (EACH-USP).

Os **peers** armazenam e transferem arquivos em _chunks_ diretamente entre si; os
**trackers (super peers)** mantêm um índice global **replicado** e sincronizado via
_flooding_ sobre TCP, com resolução de conflitos por _Last Write Wins_ e reconciliação
_anti-entropy_. A especificação completa está em [`main.tex`](main.tex) e o guia de
implementação em [`CLAUDE.md`](CLAUDE.md).

---

## 1. Tecnologias

| Camada                 | Tecnologia                                             |
| ---------------------- | ------------------------------------------------------ |
| Linguagem              | **Python 3.11+** (type hints, `match/case`)            |
| API peer ↔ tracker     | **FastAPI** + **Uvicorn** (REST sobre HTTP)            |
| Tracker ↔ tracker      | **sockets TCP brutos** (`socket` + `threading`)        |
| Peer ↔ peer            | **sockets TCP brutos** (header JSON + payload binário) |
| Persistência durável   | **SQLite** (`sqlite3`, usuários e playlists)           |
| Índice do tracker      | `dict` + `threading.Lock()` (em memória)               |
| Hashing                | `hashlib.sha256`                                       |
| Mensagens do protocolo | **pydantic v2**                                        |
| Configuração           | **YAML** (`pyyaml`)                                    |
| Testes                 | **pytest** + `httpx`                                   |

> **Sem Docker.** A rede distribuída é simulada abrindo **um terminal por nó**, cada
> processo em `127.0.0.1` numa porta distinta.

---

## 2. Pré-requisitos

- **Python 3.11 ou superior** (`python --version`).
- No Windows, instale por [python.org](https://www.python.org/) marcando
  **"Add Python to PATH"**.

### Ambiente virtual e dependências

Na raiz do projeto:

```bash
python -m venv .venv
```

Ative o ambiente:

- **Linux / macOS / Git Bash:** `source .venv/bin/activate`
- **Windows (PowerShell):** `.venv\Scripts\Activate.ps1`
  > Se o PowerShell bloquear a ativação, rode uma vez:
  > `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

Instale as dependências:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

A execução usa `PYTHONPATH=.` a partir da raiz (sem instalação editável).

---

## 3. Estrutura do projeto (resumo)

```
peerspot/
├── config/            # YAMLs dos 3 trackers e 3 peers (127.0.0.1, portas distintas)
├── scripts/           # run_tracker.sh, run_peer.sh, run_all_trackers.sh
├── src/
│   ├── common/        # messages, protocol, hashing, chunking, config, errors, logging
│   ├── tracker/       # api, index, handlers, sync_server/client, routing,
│   │                  #   tombstone, anti_entropy, failure_detector, rebalance, persistence
│   └── peer/          # cli, tracker_client, tcp_server/client, chunk_manager,
│                      #   storage, seed_reporter, downloader
├── tests/             # unit/ e integration/
├── data/  logs/       # criados em runtime (gitignored)
├── main.tex           # especificação
└── requirements.txt
```

---

## 4. Como subir o ambiente (múltiplos terminais)

O cenário padrão é **3 trackers + 3 peers = 6 terminais**. A **ordem importa**: o
**bootstrap (`tracker-1`) sobe primeiro**, pois os demais trackers se reintegram a ele
via `TRACKER_REJOIN` ao iniciar; os peers sobem por último.

Há **duas formas equivalentes** de subir cada nó — via os scripts de conveniência ou
rodando o módulo Python diretamente. Ative o `.venv` em cada terminal antes.

### 4.1. Via scripts (`scripts/`)

Os scripts ativam o `.venv` (se existir), exportam `PYTHONPATH=.` e invocam o módulo.
Um terminal por comando:

```bash
# Terminal 1 — bootstrap (PRIMEIRO!)
./scripts/run_tracker.sh 1

# Terminal 2
./scripts/run_tracker.sh 2

# Terminal 3
./scripts/run_tracker.sh 3

# Terminal 4 — peer com CLI interativa
./scripts/run_peer.sh alice

# Terminal 5
./scripts/run_peer.sh bob

# Terminal 6
./scripts/run_peer.sh carol
```

Para subir os 3 trackers de uma vez (opcional): `./scripts/run_all_trackers.sh`. Com
`tmux` disponível, abre uma sessão com 3 painéis respeitando a ordem; sem `tmux`,
apenas imprime os comandos a rodar manualmente.

### 4.2. Manualmente (`python -m …`)

Os scripts são **apenas conveniência** — tudo funciona rodando os módulos diretamente.
A partir da raiz do projeto, com `PYTHONPATH=.` exportado (`export PYTHONPATH=.` no
Linux/macOS/Git Bash ou `$env:PYTHONPATH = "."` no PowerShell), um terminal por comando:

```bash
# Terminal 1 — bootstrap (PRIMEIRO!)
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

### 4.3. Portas usadas

| Nó                    | API (REST) | Sync (TCP) | TCP de chunks |
| --------------------- | ---------- | ---------- | ------------- |
| tracker-1 (bootstrap) | 8001       | 9001       | —             |
| tracker-2             | 8002       | 9002       | —             |
| tracker-3             | 8003       | 9003       | —             |
| alice                 | —          | —          | 7001          |
| bob                   | —          | —          | 7002          |
| carol                 | —          | —          | 7003          |

---

## 5. Comandos da CLI do peer

```
help / ?                      lista os comandos
upload <caminho>              registra um arquivo de áudio na rede
search <nome>                 busca por nome legível (sem argumento: repete a última)
download <nome | nº | hash>   baixa um arquivo (faça 'search' antes)
list / ls                     lista arquivos completos neste peer
remove <nome | hash>          remove arquivo local e notifica o tracker
peers <nome | nº | hash>      fontes de um arquivo (faça 'search' antes)
playlist create <nome>        cria uma playlist (local ao peer, funciona offline)
playlist add <id> <hash>      adiciona um hash à playlist
playlist remove <id> <hash>   remove um hash da playlist
playlist show <id>            mostra o conteúdo de uma playlist
playlist list                 lista suas playlists
playlist delete <id>          apaga uma playlist
status / st                   tracker atual (com [online]/[offline]), arquivos, downloads
clear / cls                   limpa a tela do terminal
quit / exit                   sai ordenadamente (envia PEER_LEAVE)
```

> **Operação por nome legível.** `download`, `remove` e `peers` aceitam o **nome da
> música** (ou o **nº** exibido na última busca, ou o hash) — não é preciso colar o
> hash SHA-256 de 64 caracteres. `download <nome>` ainda depende de um `search`
> anterior **na mesma sessão**: é a busca que associa o nome ao hash (resolução em
> dois passos da especificação). Para arquivos locais (`list`/`remove`), o nome vem de
> um registro local persistido no `storage_dir` do peer.
>
> A saída é **colorida** quando roda num terminal (desliga sozinha ao redirecionar ou
> com `NO_COLOR`), o `download` mostra uma **barra de progresso**, e onde a stdlib
> `readline` existe (Linux/macOS/Git Bash) há **histórico** com as setas ↑/↓.

---

## 6. Roteiro de demonstração

Com os **6 nós no ar** (seção 4, bootstrap primeiro):

1. **Upload na alice** — no terminal da alice:

   ```
   alice> upload caminho/para/musica.mp3
   ```

   Anote o `hash` impresso. Em `tracker-1` o arquivo é registrado e propagado aos
   demais em menos de 3 s.

2. **Busca no bob** — no terminal do bob:

   ```
   bob> search musica
   ```

   O resultado lista, para cada arquivo, o **nº**, o nome, o número de chunks, as
   fontes (só `alice` por enquanto) e o `hash`.

3. **Download no bob** (por nome ou pelo nº da busca — não precisa colar o hash):

   ```
   bob> download musica
   ```

   Uma barra de progresso acompanha os chunks. Ao concluir (SHA-256 validado), o bob
   **se re-registra como nova fonte**.

4. **Confirmar no bob:**

   ```
   bob> list
   ```

   O arquivo baixado aparece.

5. **Download na carol** — no terminal da carol:

   ```
   carol> search musica
   carol> download musica
   ```

   Agora há **duas fontes** (`alice` e `bob`); a carol distribui os chunks entre elas
   em paralelo (_rarest-first_).

6. **Fallback de tracker** — `Ctrl+C` no terminal do `tracker-1`. Na próxima operação
   da alice (que estava conectada ao `tracker-1`), o peer **migra sozinho** para o
   `tracker-2` da sua lista:

   ```
   alice> search musica       # dispara o fallback; status confirma o novo tracker
   alice> status
   ```

7. **Reintegração do tracker** — reabra um terminal e rode `./scripts/run_tracker.sh 1`
   novamente (ou `python -m src.tracker.main --config config/tracker-1.yaml`). O
   `tracker-1` se reintegra automaticamente via
   `TRACKER_REJOIN` → `TRACKER_LIST` → `SYNC_PULL(desde_seq=0)`, reconstruindo o índice
   a partir de uma réplica viva, e parte dos peers é reassignada a ele
   (`REASSIGN_TRACKER`).

8. **Remoção de música** — na alice:

   ```
   alice> remove musica
   ```

   Em menos de 3 s os trackers registram um _tombstone_; o `bob` continua aparecendo
   como fonte.

9. **Falha desordenada de peer** — feche o terminal do `bob` **abruptamente** (sem
   `quit`). Após **6 minutos** (2 rodadas de `SEED_REPORT` perdidas), os trackers
   marcam as entradas do `bob` como _tombstone_ pelo _failure detector_.

---

## 7. Inspecionar o estado de um tracker via HTTP

Cada tracker expõe sua API REST. Exemplos com `curl`:

```bash
# Healthcheck (troque a porta por 8002 / 8003 para os outros trackers)
curl http://127.0.0.1:8001/health

# Trackers conhecidos (membership)
curl http://127.0.0.1:8001/trackers

# Buscar por nome no índice LOCAL de um tracker específico
# (ttl=0 impede o roteamento SEARCH_FORWARD: mostra só o que aquele tracker tem)
curl -X POST http://127.0.0.1:8001/search \
  -H "Content-Type: application/json" \
  -d '{"query_id":"cli","query":"musica","ttl":0}'
```

Repita o `POST /search` contra `8002` e `8003` para confirmar que a mesma entrada foi
replicada em todos os trackers.

---

## 8. Resetar o ambiente

Todo o estado de runtime vive em `data/` e `logs/` (ambos _gitignored_). Para um
recomeço limpo, basta apagá-los:

```bash
rm -rf data logs      # Windows PowerShell: Remove-Item -Recurse -Force data, logs
```

Eles são recriados automaticamente na próxima execução.

---

## 9. Limitações conhecidas

- **Playlists são locais ao peer** (persistidas em `<storage_dir>/playlists.json`),
  não estado do tracker. Ficam disponíveis mesmo sem nenhum tracker no ar (como o
  `list` de músicas) e sobrevivem a quedas/reinícios de trackers; em contrapartida,
  não são compartilhadas entre peers.
- **Lista de trackers do peer é estática** (definida no YAML). A adição dinâmica de
  trackers à lista de fallback do peer não é suportada; a limitação está registrada na
  especificação (`main.tex`, §"Sobre a lista TRACKERS no peer").
- **Sem garantia de replicação mínima de arquivos.** A replicação de conteúdo é
  orientada a demanda: um arquivo fica indisponível se o seu único peer sai da rede.
- **Modelo de falha _crash_ (fail-stop).** Não há tolerância a falhas bizantinas nem
  autenticação de peers — fora do escopo do projeto.

---

## 10. Créditos

Trabalho desenvolvido por:

- Bruno Hideo Ioneda — 15573619
- Guilherme Samuel Lemos Segura — 15575611
- Higor Ranel Viani Lopes — 15552946
- João de Melo Fantini — 15462550

Disciplina de Desenvolvimento de Sistemas de Informação Distribuídos —
Escola de Artes, Ciências e Humanidades, Universidade de São Paulo (EACH-USP).
