# scripts/lab_local.ps1
#
# Sobe um cenario tipico de teste (1 tracker + 2 peers) em paineis do
# Windows Terminal, cada um rodando em sua propria sessao PowerShell
# com o venv do projeto ativado.
#
# Pre-requisitos:
#   1. Python 3.11+ instalado e no PATH.
#   2. Venv criado em .venv na raiz do projeto:
#        python -m venv .venv
#        .\.venv\Scripts\Activate.ps1
#        pip install -e ".[dev]"
#   3. Windows Terminal (vem com Windows 11; no Windows 10 instale pela
#      Microsoft Store ou via 'winget install Microsoft.WindowsTerminal').
#   4. Politica de execucao do PowerShell permitindo scripts locais.
#      Rodar UMA VEZ como usuario (nao precisa de admin):
#        Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
#
# Como usar:
#   Da raiz do projeto:
#     .\scripts\lab_local.ps1
#   Ou clicando com o botao direito no arquivo e "Run with PowerShell".

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------
# Resolucao de caminhos
# ---------------------------------------------------------------------

# Diretorio raiz do projeto (assume que este script esta em scripts/)
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$VenvActivate = Join-Path $RepoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $VenvActivate)) {
    Write-Host ""
    Write-Host "ERRO: venv nao encontrado em:" -ForegroundColor Red
    Write-Host "  $VenvActivate" -ForegroundColor Red
    Write-Host ""
    Write-Host "Crie o venv primeiro:" -ForegroundColor Yellow
    Write-Host "  cd $RepoRoot"
    Write-Host "  python -m venv .venv"
    Write-Host "  .\.venv\Scripts\Activate.ps1"
    Write-Host "  pip install -e `".[dev]`""
    exit 1
}

# Verifica se o Windows Terminal esta instalado
$WtExe = Get-Command wt.exe -ErrorAction SilentlyContinue
if (-not $WtExe) {
    Write-Host ""
    Write-Host "ERRO: Windows Terminal (wt.exe) nao encontrado no PATH." -ForegroundColor Red
    Write-Host "Instale pela Microsoft Store ou via:" -ForegroundColor Yellow
    Write-Host "  winget install Microsoft.WindowsTerminal"
    Write-Host ""
    Write-Host "Alternativa rapida sem Windows Terminal:" -ForegroundColor Yellow
    Write-Host "  Abra 3 janelas PowerShell, ative o venv em cada uma"
    Write-Host "  (. .\.venv\Scripts\Activate.ps1) e rode os comandos manualmente."
    exit 1
}

# ---------------------------------------------------------------------
# Comandos que cada painel vai executar
# ---------------------------------------------------------------------
#
# Cada painel:
#   1. muda para a raiz do projeto
#   2. ativa o venv (dot-source com '.')
#   3. roda o processo
#
# Aspas duplas em volta dos caminhos lidam com pastas que contenham
# espacos (ex.: "C:\Users\Joao da Silva\peerspot").

$TrackerCmd = "Set-Location `"$RepoRoot`"; . `"$VenvActivate`"; " + `
              "peerspot-tracker --id tracker-1 --rest-port 8001 --mesh-port 9001"

$PeerACmd = "Set-Location `"$RepoRoot`"; . `"$VenvActivate`"; " + `
            "peerspot-peer --name peer-a --tracker http://127.0.0.1:8001"

$PeerBCmd = "Set-Location `"$RepoRoot`"; . `"$VenvActivate`"; " + `
            "peerspot-peer --name peer-b --tracker http://127.0.0.1:8001"

# ---------------------------------------------------------------------
# Abre o Windows Terminal com 3 paineis
# ---------------------------------------------------------------------
#
# Layout final:
#
#   +-------------------+-------------------+
#   |                   |   Peer A          |
#   |   Tracker         +-------------------+
#   |                   |   Peer B          |
#   +-------------------+-------------------+
#
# O backtick (`) escapa o ponto-e-virgula para que ele seja passado
# literalmente para o wt.exe (que usa ';' como separador de comandos),
# em vez de ser interpretado pelo PowerShell como separador.

wt --window 0 new-tab --title "PeerSpot Lab" `
    powershell.exe -NoExit -Command $TrackerCmd `; `
    split-pane -H --size 0.5 powershell.exe -NoExit -Command $PeerACmd `; `
    split-pane -V --size 0.5 powershell.exe -NoExit -Command $PeerBCmd

Write-Host "Lab iniciado em uma nova aba do Windows Terminal." -ForegroundColor Green
Write-Host "Para encerrar tudo, feche a aba ou pressione Ctrl+C em cada painel."