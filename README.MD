
# PeerSpot

Sistema distribuído P2P híbrido com trackers para compartilhamento de áudio, desenvolvido para a disciplina de Desenvolvimento de Sistemas de Informação Distribuídos (EACH-USP).

Este documento detalha o processo de configuração inicial do ambiente de desenvolvimento e as instruções para execução local.

## 1. Pré-requisitos

O projeto requer **Python 3.11 ou superior**. Verifique sua versão executando:
```bash
python --version

```



* **Windows:**
Baixe o instalador oficial em [python.org](https://www.python.org/) e execute-o. **Importante:** Marque a opção "Add Python to PATH" durante a instalação.

---

## 2. Configuração do Ambiente Virtual

Toda a manipulação de pacotes Python neste projeto deve ocorrer dentro de um ambiente virtual (`.venv`) isolado. Nunca instale as dependências no Python global do sistema.

**Passo 1:** Na raiz do projeto, crie o ambiente virtual:

```bash
python -m venv .venv

```

**Passo 2:** Ative o ambiente virtual:

* **Linux / macOS:**
```bash
source .venv/bin/activate

```


* **Windows (PowerShell):**
```powershell
.venv\Scripts\Activate.ps1

```



*(A partir deste ponto, o seu terminal deve mostrar `(.venv)` no início da linha de comando).*

> **Nota:** Sempre que abrir um novo terminal para trabalhar no projeto, o primeiro comando a ser executado deve ser a ativação do `.venv`.

---

## 3. Instalação das Dependências

Com o ambiente virtual ativado, atualize o gerenciador de pacotes (`pip`) e instale as dependências do projeto em modo editável:

```bash
pip install --upgrade pip
pip install -e ".[dev]"

```

A flag `-e` garante que as mudanças feitas no código sejam refletidas imediatamente, sem a necessidade de reinstalar o projeto a cada alteração.

---

## 4. Execução do Ambiente Local (Lab)

Para facilitar os testes durante o desenvolvimento, o projeto conta com um script de automação para o PowerShell que sobe um cenário típico (1 tracker + 2 peers) utilizando o **Windows Terminal**.

### Pré-requisitos para execução local (Windows):

1. **Windows Terminal:** Se você usa o Windows 10 e não o possui, instale via Microsoft Store ou via linha de comando (`winget install Microsoft.WindowsTerminal`). No Windows 11, ele já é nativo.
2. **Permissão de execução de scripts:** Rode o comando abaixo uma única vez no PowerShell (como usuário comum) para permitir a execução do script:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

```



### Subindo a rede:

Na raiz do projeto, execute o script de automação:

```powershell
.\scripts\lab_local.ps1

```
