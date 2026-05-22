# PeerSpot

Sistema distribuído P2P não estruturado hierárquico com **trackers (super peers)** para
compartilhamento de arquivos de áudio em chunks, desenvolvido para a disciplina de
Desenvolvimento de Sistemas de Informação Distribuídos (EACH-USP).

Peers armazenam e transferem chunks diretamente entre si; trackers mantêm um índice
global replicado e sincronizado via flooding sobre TCP. A especificação completa está
em [`main.tex`](main.tex) e o guia de implementação em [`CLAUDE.md`](CLAUDE.md).

> **Status:** projeto em fase inicial de implementação. A estrutura de diretórios está
> montada (§3 do `CLAUDE.md`); os módulos ainda serão preenchidos seguindo o plano de
> fases (§9). As instruções de execução multi-terminal serão detalhadas na Fase 7.

---

## 1. Pré-requisitos

O projeto requer **Python 3.11 ou superior**. Verifique sua versão:

```bash
python --version
```

No Windows, baixe o instalador oficial em [python.org](https://www.python.org/) e marque
a opção **"Add Python to PATH"** durante a instalação.

---

## 2. Configuração do Ambiente Virtual

Toda manipulação de pacotes deve ocorrer em um ambiente virtual (`.venv`) isolado.

**Passo 1 — criar o ambiente** (na raiz do projeto):

```bash
python -m venv .venv
```

**Passo 2 — ativar o ambiente:**

- **Linux / macOS:**
  ```bash
  source .venv/bin/activate
  ```
- **Windows (PowerShell):**
  ```powershell
  .venv\Scripts\Activate.ps1
  ```

A partir daqui o terminal deve exibir `(.venv)` no início da linha.

> Se o PowerShell bloquear a ativação, rode uma única vez (como usuário comum):
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

---

## 3. Instalação das Dependências

Com o `.venv` ativado:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

A execução dos nós usa `PYTHONPATH=.` a partir da raiz do projeto (sem instalação
editável), conforme o §8.3 do `CLAUDE.md`.

---

## 4. Execução

A rede distribuída é simulada abrindo **um terminal por nó** em `127.0.0.1`, cada
processo em uma porta distinta — **sem Docker**. O cenário padrão é 3 trackers + 3 peers.

As instruções completas (ordem de inicialização, comandos por terminal, configs YAML e
roteiro de demonstração) serão adicionadas aqui na Fase 7 do plano de implementação.

---

## 5. Testes

Com o ambiente ativado, a partir da raiz:

```bash
pytest -v
```
