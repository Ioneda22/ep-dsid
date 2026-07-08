#!/usr/bin/env bash
# run_all_trackers.sh — conveniência para subir os 3 trackers de uma vez (§8.3).
#
# Se o tmux estiver disponível, cria uma sessão 'peerspot' com 3 painéis (um
# por tracker), respeitando a ordem de inicialização: o bootstrap (tracker-1)
# sobe PRIMEIRO e há um pequeno atraso antes do tracker-2 e do tracker-3, pois
# eles se reintegram ao bootstrap via TRACKER_REJOIN ao subir (§8.1).
#
# Sem tmux, apenas imprime os comandos para o usuário rodar manualmente em
# 3 terminais separados. Os peers seguem em terminais próprios (run_peer.sh).
set -euo pipefail

raiz="$(cd "$(dirname "$0")/.." && pwd)"
cd "$raiz"

sessao="peerspot"

if command -v tmux >/dev/null 2>&1; then
  echo "tmux encontrado — subindo os 3 trackers na sessão '$sessao'."
  echo "Anexe com:  tmux attach -t $sessao   (Ctrl+B depois D para desanexar)"

  # Painel 0: bootstrap (tracker-1), primeiro.
  tmux new-session -d -s "$sessao" -n trackers "./scripts/run_tracker.sh 1"
  # Painel 1: tracker-2, após um respiro para o bootstrap ficar de pé.
  tmux split-window -t "$sessao":trackers -v "sleep 2; ./scripts/run_tracker.sh 2"
  # Painel 2: tracker-3.
  tmux split-window -t "$sessao":trackers -v "sleep 3; ./scripts/run_tracker.sh 3"
  tmux select-layout -t "$sessao":trackers even-vertical
  echo "Pronto. Suba os peers em outros terminais:"
  echo "  ./scripts/run_peer.sh alice"
  echo "  ./scripts/run_peer.sh bob"
  echo "  ./scripts/run_peer.sh carol"
  exit 0
fi

# --- Sem tmux: instruções manuais -------------------------------------------
cat <<'EOF'
tmux não está disponível — suba os trackers manualmente, um por terminal,
NA ORDEM (bootstrap primeiro):

  Terminal 1 (bootstrap):  ./scripts/run_tracker.sh 1
  Terminal 2:              ./scripts/run_tracker.sh 2
  Terminal 3:              ./scripts/run_tracker.sh 3

Depois, os peers, cada um em seu terminal:

  Terminal 4:              ./scripts/run_peer.sh alice
  Terminal 5:              ./scripts/run_peer.sh bob
  Terminal 6:              ./scripts/run_peer.sh carol
EOF
