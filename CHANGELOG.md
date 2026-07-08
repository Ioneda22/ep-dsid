# Changelog

Todas as mudanças notáveis do projeto PeerSpot são documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto segue [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [1.0.0] — Projeto completo

Todas as 7 fases do plano de implementação (§9 do `CLAUDE.md`) concluídas e os
**10 critérios de aceitação final (§12) verificados ✅** com os 6 nós reais.

### Adicionado
- **Fase 7 — Empacotamento de execução local e documentação** (§9 do `CLAUDE.md`):
  scripts de conveniência, README completo e verificação final ponta a ponta. **Sem
  Docker** — a execução multi-nó é feita com um terminal por processo em `127.0.0.1`
  (§8).
  - `scripts/run_tracker.sh <n>` e `scripts/run_peer.sh <nome>` — ativam o `.venv` se
    existir (`.venv/bin/activate` ou `.venv/Scripts/activate` no Git Bash/Windows),
    exportam `PYTHONPATH=.`, resolvem a raiz do projeto pelo diretório do script (rodam
    de qualquer `cwd`) e fazem `exec python -m src.{tracker,peer}.main --config …`. Todos
    os comandos permanecem acessíveis rodando `python -m …` diretamente.
  - `scripts/run_all_trackers.sh` — se houver `tmux`, sobe os 3 trackers numa sessão
    `peerspot` com 3 painéis respeitando a ordem (bootstrap primeiro, com `sleep` antes
    do tracker-2/3 pela reintegração via `TRACKER_REJOIN`); sem `tmux`, imprime os
    comandos manuais. Os três scripts com `chmod +x` (bit executável também no git).
  - `README.md` reescrito (PT-BR): tecnologias, pré-requisitos/venv, estrutura, as
    **duas formas** de subir o ambiente (scripts **e** `python -m …` manual, lado a
    lado) na ordem correta, tabela de portas, comandos da CLI, roteiro de demonstração
    de 9 passos, inspeção via `curl` (`/health`, `/trackers`, `POST /search` com
    `ttl=0` para ver o índice local de cada tracker), como rodar os testes, reset do
    ambiente e limitações conhecidas (playlists não replicadas, lista `TRACKERS`
    estática, sem replicação mínima de arquivos, modelo *crash*). Créditos com os 4
    alunos.
  - Os 6 `config/*.yaml` (§6.6/§7.6) já estavam completos das fases anteriores e foram
    validados: todos em `127.0.0.1`, portas distintas (trackers api 8001-8003 / sync
    9001-9003; peers 7001-7003), `known_trackers` de cada tracker apontando aos outros
    dois (com `sync_port` **e** `api_port`), peers listando os 3 trackers, e paths
    (`db_path`/`storage_dir`/`log_path`) relativos à raiz.

### Verificação final — Critérios de Aceitação (§12 do `CLAUDE.md`)
Executada com **3 trackers como subprocessos reais** (via o entrypoint
`python -m src.tracker.main`, onde vivem sync/reintegração/failure/rebalance) e **3
peers com os componentes reais** (`Storage`/`PeerTCPServer`/`PeerTrackerClient`/
`Downloader`), arquivo de teste de **5 MiB (20 chunks)**:

1. ✅ 3 trackers + 3 peers sobem (bootstrap primeiro) e conectam — `/health` OK nos 3.
2. ✅ `alice` faz upload de ~5 MB; `tracker-1` registra (visível no seu `POST /search`).
3. ✅ Em **0,37 s** (< 3 s) `tracker-2` e `tracker-3` têm o hash no índice local.
4. ✅ `bob` (primário `tracker-2`) busca por nome, encontra e baixa.
5. ✅ Download usou **só `alice`** como fonte; depois `bob` reaparece como 2ª fonte;
   SHA-256 do arquivo final confere.
6. ✅ `carol` (primário `tracker-3`) baixa com `alice`+`bob` como fontes disponíveis;
   SHA-256 confere (distribuição paralela *rarest-first* também coberta por
   `test_download_parallel`).
7. ✅ `Ctrl+C` (kill) no `tracker-1` → `alice` faz **fallback automático**
   `tracker-1 → tracker-2` na operação seguinte.
8. ✅ Reabrir o `tracker-1` → reintegra via `TRACKER_REJOIN` → `TRACKER_LIST` →
   `SYNC_PULL(desde_seq=0)` e **reconstrói o índice em 0,19 s** (hash de volta no t1).
9. ✅ `alice` remove o arquivo → em < 3 s `tracker-2`/`tracker-3` registram o
   *tombstone* (alice sai das fontes) e `bob` **permanece** como fonte.
10. ✅ `pytest -v` — **175 testes** passam (100%, ~30 s).

### Adicionado
- **Fase 6 — Playlists e comandos auxiliares de CLI** (§9 do `CLAUDE.md`):
  CRUD de playlists no tracker e os comandos finais do §7.2 no peer.
  - `src/tracker/persistence.py` — CRUD de playlists (antes stubs):
    `criar_playlist(dono, nome) -> id`, `listar_playlists(dono)`,
    `adicionar_item(playlist_id, hash, ordem)`, `remover_item`,
    `obter_playlist(id) -> {nome, dono, itens}`, `deletar_playlist` e o
    helper `proxima_ordem` (`MAX(ordem)+1`, robusto a remoções — evita
    colisão na PK `(playlist_id, ordem)` quando um item do meio é removido).
  - `src/tracker/api.py` — rotas REST: `POST /playlists`,
    `GET /playlists/{dono}`, `GET /playlists/{id}`,
    `POST /playlists/{id}/items`, `DELETE /playlists/{id}/items/{hash}` e
    `DELETE /playlists/{id}`. As rotas de `id` usam o convertor Starlette
    `:int` para não colidirem com `GET /playlists/{dono}` (regex `[0-9]+`
    vs. nome de peer). Corpos `CriarPlaylistBody`/`AdicionarItemBody`
    (pydantic); playlist inexistente vira `ERROR` `NOT_FOUND` (HTTP 404).
  - `src/peer/tracker_client.py` — métodos de playlist correspondentes e a
    property `trackers_conhecidos` (para o `status`). O transporte com
    fallback (§7.5) foi generalizado de `_post` para `_request(metodo, …)`,
    com `_get`/`_delete`/`_post` delegando — GET/DELETE herdam o mesmo
    fallback e a migração por `reassign_to`.
  - `src/peer/cli.py` — comandos finais do §7.2: `playlist
    create/add/remove/show/list/delete`, `peers <hash>` (refaz o
    `SEARCH_FILE` para mostrar fontes frescas; exige `search` antes, como o
    `download`, pela resolução nome→hash) e `status` (tracker atual,
    trackers conhecidos, arquivos locais e downloads em andamento —
    pendências do `ChunkManager` sem arquivo montado). Helper de módulo
    `_parse_id`.
  - Testes: `tests/unit/test_persistence.py` (criar/obter, listar por dono,
    remoção sem colisão de ordem, deletar em cascata; substitui o antigo
    teste de stub) e `tests/integration/test_playlists.py` (tracker uvicorn
    real: ciclo criar → 3 itens → listar → remover um → obter → deletar,
    mais 404 de playlist inexistente e listagem de dono sem playlists).
    Suíte completa: **175 testes**.
  - Demonstração manual: a `PeerCLI` real, dirigida contra um tracker real,
    exercitou todos os comandos de playlist, `peers <hash>` (fontes frescas
    + hash desconhecido) e `status`.

### Limitações aceitas
- **Playlists NÃO são propagadas entre trackers via `SYNC_TABLE`.** São
  dados de usuário (SQLite durável, §6.1 camada 3), locais ao tracker onde
  foram criadas — só o índice de arquivos (`nome→hash`, `hash→peers`) é
  full-replicated (main.tex §11.1). Consequências assumidas: (1) uma
  playlist criada no `tracker-1` não é visível ao consultar o `tracker-2`;
  (2) se o peer sofre fallback (§7.5) ou é reassignado (rebalance, Fase 5)
  para outro tracker, suas playlists não o acompanham. É aceitável no
  escopo do projeto: playlists são conveniência de apresentação, não estado
  crítico do sistema distribuído; replicá-las exigiria estendê-las ao
  protocolo de sincronização (`SYNC_TABLE`/LWW), fora do pedido da Fase 6.

### Alterado
- **Eventos multi-hash agora carimbam um único `timestamp` (além do único
  `seq`)**: `Index.remove_peer` (PEER_LEAVE), `apply_seed_hashes` (reconciliação
  do SEED_REPORT) e o novo `detectar_peers_falhos` tocam vários pares
  (hash, peer) sob UM `seq` e UM `timestamp` capturados uma vez por evento. Sem
  isso, a `SYNC_TABLE` de propagação (timestamp no nível da mensagem) divergiria
  do que ficou gravado localmente e o LWW não convergiria entre réplicas.
  `_registrar_fonte_locked`/`_tombstone_locked` ganharam um parâmetro opcional
  `timestamp`; novo helper `_tombstonar_para_delta_locked` reúne tombstone +
  `SyncTableEntry` num passo. `remove_peer` e `apply_seed_hashes` passaram a
  devolver um `LocalDelta(seq, timestamp, entries)` para o handler propagar.
- **`PEER_LEAVE` e a reconciliação do `SEED_REPORT` agora PROPAGAM via
  `SYNC_TABLE`** (antes eram limitações conhecidas da Fase 4): `handle_peer_leave`
  e `handle_seed_report` recebem o `SyncClient` e floodam o `LocalDelta`
  resultante. A detecção por `seq`/digest continua como backstop.
- **`AckOk` passou de `dict[str, str]` para `dict[str, Any]`**: as respostas de
  escrita podem carregar `reassign_to` (objeto `{ip, api_port}`) quando o
  rebalance agendou uma migração para aquele peer. Rotas de `api.py` anotadas
  como `dict[str, Any]`.
- **`config/tracker-{1,2,3}.yaml`: `known_trackers` ganhou `api_port`** por
  entrada — o rebalance precisa do endereço REST do tracker reintegrado para
  dizer ao peer cedido onde se reportar (o sync só conhecia `sync_port`).

### Adicionado
- **Fase 5 — Robustez e dinamismo** (§9 do `CLAUDE.md`): failure detector,
  SEED_REPORT real, fallback de tracker, download paralelo e rebalance por
  `REASSIGN_TRACKER`. A reintegração (`TRACKER_REJOIN` → `TRACKER_LIST` →
  `SYNC_PULL(desde_seq=0)`) já vinha do refactor da reconciliação; aqui entrou a
  cessão de peers. **NÃO** houve reintrodução de `FULL_SYNC`.
  - `src/peer/seed_reporter.py` — `SeedReporter` real (antes stub): thread que a
    cada `seed_report_interval_seconds` (180s) envia `SEED_REPORT` com
    `storage.list_local_files()` pelo `PeerTrackerClient` (respeitando o
    fallback). `enviar_agora()` para inicialização/testes (§10).
  - `src/tracker/failure_detector.py` (novo) — `FailureDetector`: thread que a
    cada 60s chama `Index.detectar_peers_falhos(timeout)`; peer sem `SEED_REPORT`
    há > `seed_report_timeout_seconds` (360s = 2 rodadas) sai da presença, tem
    todas as fontes tombstonadas (um `seq`/`timestamp` por peer) e a remoção é
    propagada via `SYNC_TABLE`. Espelha o `TombstoneReaper` (relógio injetável).
  - `src/tracker/index.py` — `detectar_peers_falhos`, `listar_peers_locais`,
    `agendar_reassign`/`consumir_reassign` (fila de migrações pendentes por peer)
    e o `LocalDelta`. `apply_sync_entry` e a lógica de `seq`/pendência/digest
    ficaram **inalterados**.
  - `src/peer/tracker_client.py` — fallback COMPLETO (§7.5): lista `trackers` do
    YAML, `current_tracker_index` que não reseta, avanço em timeout/conexão
    recusada com `PEER_HELLO` automático ao novo tracker, e
    `TodosTrackersIndisponiveis` quando todos falham. Migração por `reassign_to`
    na resposta (entrega do `REASSIGN_TRACKER` sobre REST, sem push tracker→peer).
  - `src/peer/downloader.py` — download **paralelo** (substitui o sequencial da
    Fase 3): `CHUNK_LIST_REQUEST` em paralelo, plano rarest-first (chunk raro
    primeiro, fonte de menor carga como primária + fallbacks) e
    `ThreadPoolExecutor(download_pool_size)`; chunk sem fonte que sirva falha o
    download. Retomada a partir do disco preservada.
  - `src/peer/tcp_client.py` — thread-safe para o pool: um lock POR DESTINO
    (fontes distintas em paralelo; mesma fonte serializa na sua conexão única) +
    `_dict_lock` para o cache de conexões.
  - `src/tracker/rebalance.py` (novo) — `RebalanceManager`: ao processar
    `TRACKER_REJOIN`/`TRACKER_ANNOUNCE`, cada tracker ativo cede
    `floor(meus_peers_locais / N_trackers)` peers ao reintegrado via
    `agendar_reassign` (endereço REST vindo do `api_por_tracker_id` do YAML).
    `SyncServer` ganhou o gatilho `_ceder_peers`.
  - Hooks: `handle_peer_hello/seed_report/register_file/peer_leave_file` anexam
    `reassign_to` quando há migração pendente; `src/tracker/main.py` sobe o
    `FailureDetector` e o `RebalanceManager`; `src/peer/main.py` liga o
    `SeedReporter` real e o `download_pool_size`; `src/peer/cli.py` trata
    `TodosTrackersIndisponiveis`. `config/peer-carol.yaml` criado (3º peer).
  - Testes (6 novos): `tests/integration/test_seed_report.py` (hash omitido no
    relatório vira tombstone), `test_failure_detection.py` (relógio injetado +7
    min → tombstone + `SYNC_TABLE` com `seq`; `SyncClient` fake),
    `test_fallback.py` (2 trackers reais: tracker cai → peer migra e se
    reapresenta), `test_download_parallel.py` (alice 0-3 / bob 2-5 → carol usa as
    duas fontes, SHA-256 confere), `test_tracker_rejoin.py` (rebalance:
    `floor(3/3)=1` cedido, peer migra via `reassign_to` REST; e caso
    `floor(1/3)=0` não cede). `tests/integration/cluster.py` liga o
    `RebalanceManager` (mapa de `api_port` pré-resolvido). Suíte completa:
    **167 testes** (~27 s).
  - Demonstração manual com 3 trackers em processos reais (bootstrap primeiro):
    upload de 5 MB (20 chunks) na alice via tracker-1 replicou aos trackers 2 e 3
    em 0,38 s; carol baixou distribuindo **10 chunks de alice + 10 de bob** em
    paralelo (SHA-256 conferido); ao derrubar o tracker-1, um peer migrou sozinho
    para o tracker-2; ao reabrir o tracker-1, ele reintegrou via `TRACKER_REJOIN`
    → `TRACKER_LIST` → `SYNC_PULL(desde_seq=0)` e reconstruiu o índice (3 entradas,
    3 origens — sem reenviar o índice inteiro).
- **`SYNC_TABLE` ganhou `nome`/`tamanho`/`n_chunks` opcionais por entry**
  (extensão consciente do Listing 7.2, refletida no `main.tex`, decidida
  com o usuário): sem os metadados, um tracker que conhece o hash apenas
  via flooding não consegue responder buscas por nome localmente (quebra
  o critério §12.3 do `CLAUDE.md`) nem aceitar o re-registro
  pós-download de um peer local, que envia só o hash (quebra o §12.5).
  O `FULL_SYNC` já carregava esses campos — a omissão no `SYNC_TABLE`
  era inconsistente. Tombstones (`ativo: False`) os omitem.
- **`Index.register_file` e `Index.remove_peer_from_hash` passaram a
  devolver cópias da entrada/tombstone gravados**: o handler monta o
  `SYNC_TABLE` de propagação com o MESMO timestamp persistido no índice
  local — se cada réplica recebesse um timestamp diferente do gravado no
  originador, o LWW não convergiria.
- **`PeerEntry`/`TombstoneEntry` ganharam o campo interno `origem`**
  (tracker que produziu a escrita) e `Index` recebe `tracker_id` no
  construtor para marcar escritas locais. Motivo: o desempate do LWW
  ("empate → maior `tracker_id`", main.tex §12.2) precisa comparar os
  tracker_ids dos DOIS escritores; comparar só com o id do tracker local
  tornaria o vencedor dependente da ordem de chegada e as réplicas
  divergiriam. É estrutura interna do índice, não muda o protocolo.
- **`SEARCH_RESULT` ganhou `n_chunks` por entrada** (extensão consciente
  do Listing 7.2, refletida no `main.tex`): o peer precisa do total de
  chunks para montar o plano de download e o tracker já o conhece do
  `REGISTER_FILE` original — sem o campo, o peer teria de inferir o
  total a partir dos `CHUNK_LIST` das fontes, o que falharia se nenhuma
  fonte tivesse o arquivo completo. Alterados `SearchResultEntry`
  (`src/common/messages.py`), `Index.search_by_name`
  (`src/tracker/index.py`) e o Listing 7.2 do `main.tex`.
- Código formatado com `black` (§14.5 do `CLAUDE.md`).
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
- **Reconciliação entre trackers por número de sequência (`seq`) + reparo
  direcionado** (refactor da anti-entropy; **substitui o `FULL_SYNC`
  periódico**, refletido no `main.tex` §11.3): o `SYNC_TABLE` continua
  incremental e *best-effort*, mas agora carrega um `seq` monotônico por
  origem. Cada tracker guarda a proveniência `(origem, seq)` de cada entrada e
  um **vetor de versões** `visto[origem]`; a perda de um delta é **detectada**
  (lacuna de `seq`) e reparada puxando só o que faltou, não reenviando o índice
  inteiro — custo O(divergência), não O(índice) por rodada. O `seq` apenas
  detecta; o LWW por `timestamp` continua a única autoridade de conflito
  (`apply_sync_entry` inalterado, só ganhou o `seq` como proveniência).
  - `src/common/messages.py` — `SYNC_TABLE` ganhou `seq`; novas mensagens
    `SYNC_DIGEST` (vetor de versões), `SYNC_PULL` (`faltando: [{origem,
    desde_seq}]`) e `TRACKER_LIST` (só a membership); `MESSAGE_MODELS` passa de
    19 a 21 tipos. Refletido no Listing 7.2 do `main.tex`.
  - `src/tracker/index.py` — `PeerEntry`/`TombstoneEntry` ganharam `seq`
    (proveniência obrigatória, junto de `origem`); `meu_seq` (== `visto[meu_id]`),
    o vetor `visto` e o conjunto de **pendências** vivem sob o MESMO lock. Novos
    métodos: `registrar_recepcao_flood` (detecção de lacuna + `visto` por `max` +
    pendência com o `desde_seq` capturado no instante), `avancar_visto` (reparo/
    reconstrução, sem abrir pendência), `resolver_pendencia`, `versoes`,
    `pendencias`, `comparar_digest` e `selecionar_para_pull` (resposta agrupada
    por `seq`, com tombstones, a partir só do estado atual — sem log histórico).
  - `src/tracker/sync_server.py` — `SYNC_TABLE` detecta lacuna e dispara
    `SYNC_PULL(desde_seq capturado)`; novos handlers `SYNC_PULL` (responde uma
    `SYNC_TABLE` por `seq` na mesma conexão TCP e fecha) e `SYNC_DIGEST` (compara
    e puxa); `TRACKER_REJOIN` responde `TRACKER_LIST` + propaga `TRACKER_ANNOUNCE`.
  - `src/tracker/sync_client.py` — `propagar_sync` carrega `seq`; novos
    `propagar_digest`, `solicitar_pull`/`solicitar_pull_de` (aplica as respostas
    via `avancar_visto` + `apply_sync_entry` e fecha a pendência) e `reintegrar`
    (`TRACKER_REJOIN` → `TRACKER_LIST` → `SYNC_PULL(desde_seq=0)` pelo primeiro
    conhecido reachable, inicializando `visto`/`meu_seq` sem persistir em disco).
  - `src/tracker/anti_entropy.py` — `DigestBroadcaster`: thread que faz *push* de
    `SYNC_DIGEST` a cada `digest_interval_seconds` (default **300s = 5 min**, <
    retenção do tombstone), backstop para a última escrita perdida + silêncio.
  - `src/tracker/main.py` sobe o `DigestBroadcaster` e dispara a reintegração em
    background; `digest_interval_seconds` entra em `TrackerSettings` e nos
    `config/tracker-{1,2,3}.yaml` (substitui `anti_entropy_interval_seconds`).
  - Testes: `tests/unit/test_seq_pull.py` (alocação de `seq`, proveniência sem
    afetar o LWW, detecção de lacuna com `desde_seq` capturado, `visto` por `max`,
    pendências, `comparar_digest`, resposta de pull agrupada por `seq` com
    tombstones) e `tests/integration/test_sync_repair.py` (3 trackers reais:
    lacuna inline reparada por `SYNC_PULL`, última escrita reposta pelo
    `SYNC_DIGEST`, reintegração reconstrói o índice e inicializa `visto`/`meu_seq`).
    Suíte completa: 161 testes.
- **Reconciliação anti-entropy periódica entre trackers** (fecha o buraco de
  entropia do `SYNC_TABLE`, decidido com o usuário e refletido no `main.tex`
  §"Reconciliação anti-entropy"): o `SYNC_TABLE` é incremental e
  *best-effort* (sem retransmissão), então um tracker que perca um delta mas
  continue no ar ficaria desatualizado para sempre. A correção é um *push*
  periódico do estado completo via `FULL_SYNC`, reaplicado por LWW
  (idempotente), que repara o que divergiu independentemente da causa.
  Decidimos por anti-entropy em vez de ACK+retransmissão no `SYNC_TABLE`
  porque um único mecanismo cobre também os casos que o ACK não cobriria
  (queda do remetente antes de entregar; destino fora do ar por muito tempo).
  - `src/common/messages.py` — `FullSyncPeer` ganhou `origem` (opcional,
    default `""`) para o receptor aplicar o desempate do LWW de forma
    determinística (mesmo critério do `SyncTableEntry`). Refletido no Listing
    7.2 do `main.tex`.
  - `src/tracker/index.py` — novo `apply_full_sync(entries)`: converte cada
    `FullSyncPeer` numa escrita LWW e reaproveita `apply_sync_entry`
    (timestamp/origem viajam **por peer** no `FULL_SYNC`, não no nível da
    mensagem). Idempotente: reaplicar o mesmo estado não muda nada.
  - `src/tracker/sync_client.py` — `propagar_full_sync(snapshot)`: *push*
    paralelo (uma thread daemon por destino, *best-effort* como o
    `propagar_sync`); `_entries_do_snapshot` agora carrega `origem` das
    fontes e tombstones.
  - `src/tracker/sync_server.py` — `FULL_SYNC` deixou de ser ignorado: é
    aplicado via `Index.apply_full_sync` (eco do próprio `origem` é
    descartado). `TRACKER_REJOIN`/`TRACKER_ANNOUNCE` seguem reconhecidos e
    logados até a Fase 5.
  - `src/tracker/anti_entropy.py` (novo) — `AntiEntropyReconciler`: thread
    daemon que faz `propagar_full_sync` a cada `anti_entropy_interval_seconds`
    (espelha o padrão do `TombstoneReaper`); `reconciliar_agora()` força um
    ciclo imediato (inicialização/testes §10).
  - `src/tracker/main.py` sobe o reconciler junto do sync server e do reaper;
    `anti_entropy_interval_seconds` (default **180s = 3 min**, < retenção do
    tombstone) entra em `TrackerSettings` e nos `config/tracker-{1,2,3}.yaml`.
  - Testes: `tests/unit/test_full_sync.py` (apply_full_sync: registra fonte +
    metadata, aplica tombstone, LWW descarta versão mais antiga, idempotência,
    desempate por origem) e `tests/integration/test_anti_entropy.py` (3
    trackers reais: delta e tombstone perdidos pelo flooding são repostos pelo
    push de `FULL_SYNC`; versão local mais nova não é sobrescrita).
- **Fase 4 — Sincronização entre trackers** (§9 do `CLAUDE.md`): flooding
  `SYNC_TABLE` sobre TCP unicast com `socket` + `threading` (NUNCA
  asyncio, §11.2), LWW, tombstones com expiração e `SEARCH_FORWARD`.
  `TRACKER_REJOIN`/`FULL_SYNC`/`TRACKER_ANNOUNCE` ficam para a Fase 5
  (o sync server os reconhece e loga, sem processar):
  - `src/tracker/index.py` — `apply_sync_entry(entry, origem_tracker,
    timestamp)` com LWW (§6.2): timestamp maior vence; menor é
    descartado; empate vence o maior `tracker_id` lexicográfico,
    comparando a `origem` da versão local com a do remetente — resolução
    determinística em qualquer ordem de chegada. O `timestamp` é
    parâmetro (não campo da entry) porque o Listing 7.2 o define no
    nível da mensagem `SYNC_TABLE`. `ativo=False` vira tombstone — e é
    gravado MESMO sem fonte local prévia, para barrar um registro
    atrasado (timestamp menor) que chegue depois da remoção (raison
    d'être do tombstone, main.tex §12.3). `ativo=True` sobre tombstone
    mais antigo remove o tombstone e registra a fonte. A tabela
    `nome_peer_to_endereco` NÃO é tocada pelo sync: presença/failure
    detection é responsabilidade do tracker ao qual o peer reporta
    SEED_REPORT; a entry carrega ip/porta, suficiente para buscas.
    `expire_tombstones(retention)` remove tombstones com idade acima da
    retenção (sem deixar dicts vazios órfãos).
  - `src/tracker/sync_server.py` — `SyncServer`: servidor TCP na
    `sync_port` dedicada (espelha o Listing 8.1), SEPARADO do FastAPI
    mas no mesmo processo; `accept()` em loop, uma thread por conexão,
    `MessageReader` por conexão (suporta o one-shot do flooding e o
    request/response do SEARCH_FORWARD). `SYNC_TABLE` com
    `origem == tracker_id` próprio é ignorada como eco (papel do campo
    `origem`, main.tex §10). `sync_port=0` escolhe porta livre (testes).
  - `src/tracker/sync_client.py` — `SyncClient.propagar_sync`: uma
    thread daemon por tracker conhecido (flooding paralelo, Listing
    8.1), fire-and-forget. Falha de conexão marca o destino como
    suspeito e NÃO retransmite — reconciliação via SEED_REPORT
    (anti-entropy) ou FULL_SYNC na reintegração (Fase 5); sucesso
    desmarca. `enviar_full_sync` (montagem do FULL_SYNC a partir do
    `IndexSnapshot`, incluindo tombstones com `ativo=False`) já pronto
    para a Fase 5.
  - `src/tracker/routing.py` — `SearchRouter` (§6.4): busca local
    primeiro; sem hit e `ttl > 0`, envia `SEARCH_FORWARD(ttl-1)` em
    paralelo aos trackers ainda não consultados (cache LRU
    `query_id → set[tracker_id]`, 1024 entradas) e agrega os
    `SEARCH_RESULT` que chegarem em até `search_forward_timeout_seconds`
    (2s); atrasados são descartados; sem nada, `resultados=[]`.
    Resultados de múltiplos trackers são mesclados por hash (união dos
    peers). **Decisão**: o `SEARCH_RESULT` volta NA MESMA conexão TCP do
    `SEARCH_FORWARD` — em topologia totalmente conectada a conexão parte
    do próprio tracker de origem, então responder nela É "devolver
    direto ao origem_tracker" (Listing 7.2), sem dispatcher de respostas
    por query_id. `handle_search_forward` (receptor) só busca localmente
    e nunca re-encaminha: o originador já consulta todos de uma vez
    (mesma razão do "não é preciso re-flood" do main.tex §8).
  - `src/tracker/tombstone.py` — `TombstoneReaper`: thread daemon que
    chama `Index.expire_tombstones` a cada 60s; retenção de 600s (10
    min) vem de `tombstone_retention_seconds` do YAML. Intervalo e
    relógio injetáveis (§10).
  - Hooks (handlers/api/main): `REGISTER_FILE` e `PEER_LEAVE_FILE`
    propagam via `propagar_sync` após a escrita local, sem bloquear a
    resposta REST (threads daemon); o tombstone propaga com
    `ativo: False` e timestamp local. `/search` usa o `SearchRouter`.
    `SyncClient`/`SearchRouter` são opcionais no `create_app` (§14.4) —
    `None` mantém o comportamento isolado da Fase 2. `main.py` sobe
    sync server + reaper antes do uvicorn. **Limitação conhecida**: os
    tombstones gerados por `PEER_LEAVE` e pelo anti-entropy do
    `SEED_REPORT` ainda não propagam (fora do escopo pedido na Fase 4;
    entram com o failure detector na Fase 5).
  - `config/tracker-2.yaml` e `config/tracker-3.yaml` (portas
    8002/9002 e 8003/9003); apenas tracker-1 com `is_bootstrap: true`.
  - Testes: `tests/integration/test_lww.py` (timestamp maior vence,
    menor perde, empate vence maior tracker_id nas duas ordens de
    chegada, replay idêntico descartado, registro novo remove tombstone,
    registro atrasado não ressuscita removido),
    `tests/integration/test_tombstone.py` (SYNC_TABLE `ativo=False` vira
    tombstone, expiração só após 600s, seletiva por idade, reaper em
    background — relógio fake §10), `tests/integration/test_sync_flooding.py`
    (3 trackers reais em portas dinâmicas: REGISTER_FILE no tracker-1
    aparece nos índices LOCAIS de 2 e 3 em < 3s com `hash_to_peers`
    idêntico nas três réplicas; PEER_LEAVE_FILE propaga tombstone
    preservando a outra fonte; tracker morto não trava o REGISTER_FILE
    e vira suspeito) e `tests/integration/test_search_forward.py`
    (cluster SEM flooding: busca no tracker-3 roteia ao tracker-1 e
    encontra; `ttl=0` não roteia; fonte caída degrada para `[]` dentro
    do timeout). Helper `tests/integration/cluster.py` sobe N trackers
    completos (Index + SyncServer + uvicorn) em threads. Suíte
    completa: 144 testes em ~24 s.
- Demonstração manual executada: 3 trackers em processos separados
  (bootstrap primeiro), REGISTER_FILE via REST no tracker-1, busca nos
  trackers 2 e 3 retornando o hash em ~2s; tracker-2 morto, novo
  REGISTER_FILE no tracker-1 respondeu em 62 ms (sem travar) e o log
  registrou `destino=tracker-2 ... marcado suspeito`, com o tracker-3
  recebendo a atualização normalmente.
- **Fase 3 — Peer básico** (§9 do `CLAUDE.md`): UM peer com download
  SEQUENCIAL (paralelo só na Fase 5), sem fallback de tracker (Fase 5)
  e sem sync entre trackers (Fase 4):
  - `src/peer/storage.py` — classe `Storage` com layout
    `<hash>/chunks/chunk_<i>.bin` (durante download) e `<hash>/arquivo`
    (montado). Para não duplicar espaço, `assemble_file` valida o
    SHA-256 (monta em `arquivo.tmp` + rename, para `load_chunk` nunca
    ver montagem parcial) e **apaga os chunks individuais**; ao servir
    outros peers, `load_chunk` extrai a fatia do arquivo montado
    on-the-fly (`seek`/`read`). Métodos: `save_chunk`, `load_chunk`,
    `has_chunk`, `get_chunk_count`, `list_local_files`, `import_file`
    (upload: hash + cópia para o storage), `assemble_file`,
    `remove_file`, `assembled_path`. `chunk_size` injetável.
  - `src/peer/tracker_client.py` — `PeerTrackerClient` (httpx síncrono,
    timeout configurável), usa só o primeiro tracker da lista (fallback
    na Fase 5): `peer_hello`, `peer_leave`, `seed_report`,
    `register_file`, `search_file` (retorna `SearchResult` tipado),
    `peer_leave_file`. Falhas de rede logam e retornam `None`.
  - `src/peer/tcp_server.py` — `PeerTCPServer` em thread daemon, uma
    thread filha por conexão; conexões peer↔peer são **persistentes**
    (um `MessageReader` por conexão, loop até o cliente fechar). Atende
    `CHUNK_LIST_REQUEST` → `CHUNK_LIST` e `CHUNK_REQUEST` →
    `CHUNK_DATA_HEADER` + payload (`send_chunk`); pedidos malformados ou
    chunk ausente respondem `ERROR` (`MALFORMED_MESSAGE`/`NOT_FOUND`).
    `porta=0` escolhe porta livre (testes); `started: threading.Event`.
  - `src/peer/tcp_client.py` — `PeerTCPClient` com cache de conexões
    persistentes por fonte (reuso entre múltiplos `CHUNK_REQUEST`):
    `request_chunk_list`, `download_chunk`, `close_all`; timeout por
    requisição (`chunk_request_timeout_seconds`), conexão descartada em
    falha.
  - `src/peer/chunk_manager.py` — `ChunkManager`: `start_download`,
    `mark_received`, `missing_chunks`, `is_complete`, `progress`,
    `reset`.
  - `src/peer/downloader.py` — `Downloader` sequencial: `SEARCH_FILE`
    (por nome; o `SEARCH_RESULT` traz `n_chunks`) → `CHUNK_LIST_REQUEST`
    a cada fonte → chunks em ordem, qualquer fonte que tenha o chunk;
    em falha tenta a próxima fonte e, se todas falharem, **retenta com
    espera** (`max_tentativas`/`retry_delay_seconds`, `sleep` injetável
    §10) → `assemble_file` valida SHA-256 → re-registro. **Retomada**:
    chunks já gravados são mantidos em falha e o progresso é
    reconciliado a partir do disco (`has_chunk`), sobrevivendo a
    reinício do processo; hash divergente descarta tudo (§7.4 passo 6).
  - `src/peer/cli.py` — `PeerCLI` com `input()`/`print()` (§7.2):
    `help`, `upload`, `search`, `download`, `list`, `remove`, `quit`;
    `peers`/`playlist`/`status` são stubs até as Fases 5/6. `download
    <hash>` usa o nome legível cacheado da última busca (resolução em
    dois passos do main.tex).
  - `src/peer/seed_reporter.py` — stub: thread daemon que apenas dorme
    em ciclos de 3 min (envio real do `SEED_REPORT` na Fase 5).
  - `src/peer/main.py` — entrypoint
    `python -m src.peer.main --config config/peer-alice.yaml`
    (`PeerSettings` do YAML §7.6): sobe TCP server + seed reporter,
    envia `PEER_HELLO`, roda a CLI; no `quit` envia `PEER_LEAVE` e
    encerra threads/conexões.
  - `config/peer-alice.yaml` (porta 7001) e `config/peer-bob.yaml`
    (porta 7002), ambos com a lista dos 3 trackers (§7.6).
  - Testes: `tests/unit/test_storage.py` (12),
    `tests/unit/test_chunk_manager.py` (7) e
    `tests/integration/test_download_sequential.py` — tracker uvicorn
    real + componentes de alice e bob em portas dinâmicas (§10): upload
    de 2 MiB (8 chunks) em alice, download sequencial em bob com SHA-256
    conferido byte a byte, chunks apagados pós-montagem, bob vira
    segunda fonte no índice e passa a servir chunks fatiados do arquivo
    montado. Suíte completa: 121 testes em ~7 s.
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
- **`FULL_SYNC` removido por completo** — substituído pelo esquema `seq` +
  `SYNC_PULL` / `SYNC_DIGEST` acima. Saíram: a mensagem `FULL_SYNC` e os modelos
  `FullSync*` (`src/common/messages.py`), o `Index.apply_full_sync`, o
  `SyncClient.propagar_full_sync`/`enviar_full_sync`/`_entries_do_snapshot`, o
  handler no `sync_server` e o *push* periódico do índice inteiro
  (`AntiEntropyReconciler` → agora `DigestBroadcaster`). Também removidos os
  testes `tests/unit/test_full_sync.py` e `tests/integration/test_anti_entropy.py`.
  Nenhuma referência a `FULL_SYNC` resta no código.
- Layout plano antigo (`common/`, `peer/`, `tracker/`), `pyproject.toml`,
  `peerspot.egg-info/`, `main.py` de exemplo, `config/trackers.json` e
  `scripts/lab.local.ps1`, substituídos pela estrutura do §3.
