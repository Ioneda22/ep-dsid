#!/usr/bin/env bash
# run_peer.sh — sobe um peer do PeerSpot (com CLI interativa) num terminal (§8.3).
#
# Uso: ./scripts/run_peer.sh <nome>
#   ex.: ./scripts/run_peer.sh alice
#        ./scripts/run_peer.sh bob
#        ./scripts/run_peer.sh carol
#
# É apenas uma conveniência: equivale a rodar, da raiz do projeto,
#   PYTHONPATH=. python -m src.peer.main --config config/peer-<nome>.yaml
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Uso: $0 <nome>   (ex.: $0 alice)" >&2
  exit 1
fi

nome="$1"
# Raiz do projeto = diretório-pai deste script (funciona de qualquer cwd).
raiz="$(cd "$(dirname "$0")/.." && pwd)"
cd "$raiz"

config="config/peer-${nome}.yaml"
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
exec python -m src.peer.main --config "$config"
