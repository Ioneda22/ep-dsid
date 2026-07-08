#!/usr/bin/env bash
# run_tracker.sh — sobe um tracker do PeerSpot num terminal (§8.3 do CLAUDE.md).
#
# Uso: ./scripts/run_tracker.sh <n>
#   ex.: ./scripts/run_tracker.sh 1   (bootstrap — suba PRIMEIRO)
#        ./scripts/run_tracker.sh 2
#        ./scripts/run_tracker.sh 3
#
# É apenas uma conveniência: equivale a rodar, da raiz do projeto,
#   PYTHONPATH=. python -m src.tracker.main --config config/tracker-<n>.yaml
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Uso: $0 <n>   (ex.: $0 1)" >&2
  exit 1
fi

n="$1"
# Raiz do projeto = diretório-pai deste script (funciona de qualquer cwd).
raiz="$(cd "$(dirname "$0")/.." && pwd)"
cd "$raiz"

config="config/tracker-${n}.yaml"
if [ ! -f "$config" ]; then
  echo "Config não encontrado: $config" >&2
  exit 1
fi

# Ativa o virtualenv se existir (Linux/macOS ou Git Bash no Windows).
if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
  # shellcheck disable=SC1091
  source .venv/Scripts/activate
fi

export PYTHONPATH=.
exec python -m src.tracker.main --config "$config"
