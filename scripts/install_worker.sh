#!/usr/bin/env bash
# Install prefect_sweep_mcp into ~/.prefect_sweep_mcp and start a Prefect worker.
#
# Reads from env (or prompts):
#   PREFECT_SWEEP_MCP_REPO   default: git@github.com:Suchun-sv/prefect_sweep_mcp.git
#   PREFECT_SWEEP_MCP_BRANCH default: main
#   PREFECT_API_URL          required, e.g. http://your-prefect-host:4200/api
#   WORK_POOL                required, e.g. CPU_pool
#   WORK_QUEUE               optional, e.g. practice
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/Suchun-sv/prefect_sweep_mcp/main/scripts/install_worker.sh | bash
#   # or after cloning:
#   bash scripts/install_worker.sh

set -euo pipefail

INSTALL_DIR="${PREFECT_SWEEP_MCP_HOME:-$HOME/.prefect_sweep_mcp}"
REPO_URL="${PREFECT_SWEEP_MCP_REPO:-git@github.com:Suchun-sv/prefect_sweep_mcp.git}"
BRANCH="${PREFECT_SWEEP_MCP_BRANCH:-main}"

# Read prompts from /dev/tty so this works under `curl ... | bash`,
# where stdin is the pipe (not the terminal).
if [[ -r /dev/tty ]]; then
  TTY_IN=/dev/tty
else
  TTY_IN=/dev/stdin
fi

prompt_if_unset() {
  local var_name="$1"
  local prompt_text="$2"
  local default_value="${3:-}"
  local value=""
  if [[ -z "${!var_name:-}" ]]; then
    if [[ -n "$default_value" ]]; then
      read -r -p "$prompt_text [$default_value]: " value < "$TTY_IN" || true
      value="${value:-$default_value}"
    else
      read -r -p "$prompt_text: " value < "$TTY_IN" || true
    fi
    if [[ -z "$value" ]]; then
      echo "ERROR: $var_name is required (set it via env or run interactively)" >&2
      exit 1
    fi
    printf -v "$var_name" '%s' "$value"
    export "$var_name"
  fi
}

prompt_if_unset PREFECT_API_URL "Prefect API URL (e.g. http://host:4200/api)"
prompt_if_unset WORK_POOL "Work pool name (e.g. CPU_pool)"
# WORK_QUEUE is optional — leave blank to listen on all queues in the pool
if [[ -z "${WORK_QUEUE:-}" ]]; then
  read -r -p "Work queue name (optional, blank = all queues): " WORK_QUEUE < "$TTY_IN" || true
fi
# WORKER_LIMIT caps concurrent flow runs the worker will pick up (1 = serial)
if [[ -z "${WORKER_LIMIT:-}" ]]; then
  read -r -p "Concurrent run limit [1]: " WORKER_LIMIT < "$TTY_IN" || true
  WORKER_LIMIT="${WORKER_LIMIT:-1}"
fi
if ! [[ "$WORKER_LIMIT" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: WORKER_LIMIT must be a positive integer (got '$WORKER_LIMIT')" >&2
  exit 1
fi
# GITHUB_TOKEN is optional — only needed if the worker has to clone private
# repos (or private transitive deps via uv). When set, we install three
# git config insteadOf rules so any git@github.com:/ssh:/git+ssh: URL is
# transparently rewritten to https://x-access-token:<token>@github.com/.
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  read -r -p "GitHub PAT for private repos (optional, blank to skip): " GITHUB_TOKEN < "$TTY_IN" || true
fi

echo "==> Install dir:   $INSTALL_DIR"
echo "==> Prefect API:   $PREFECT_API_URL"
echo "==> Work pool:     $WORK_POOL"
echo "==> Work queue:    ${WORK_QUEUE:-<all>}"
echo "==> Worker limit:  $WORKER_LIMIT"
echo "==> GitHub token:  ${GITHUB_TOKEN:+<set>}${GITHUB_TOKEN:-<unset>}"

# 1. Get repo into INSTALL_DIR
if [[ -d "$INSTALL_DIR/.git" ]]; then
  echo "==> Updating existing checkout in $INSTALL_DIR"
  git -C "$INSTALL_DIR" fetch --depth 1 origin "$BRANCH"
  git -C "$INSTALL_DIR" checkout "$BRANCH"
  git -C "$INSTALL_DIR" reset --hard "origin/$BRANCH"
else
  echo "==> Cloning $REPO_URL@$BRANCH -> $INSTALL_DIR"
  mkdir -p "$(dirname "$INSTALL_DIR")"
  git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$INSTALL_DIR"
fi

# 2. Ensure uv is installed
if ! command -v uv >/dev/null 2>&1; then
  echo "==> Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # shellcheck disable=SC1091
  export PATH="$HOME/.local/bin:$PATH"
fi

# 2b. If a GitHub PAT is provided, rewrite SSH-style github URLs to HTTPS+token
# so uv (and anything else calling git) can fetch private deps without keys.
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  echo "==> Configuring git insteadOf rules for github.com (token auth)"
  rewrite="https://x-access-token:${GITHUB_TOKEN}@github.com/"
  git config --global "url.${rewrite}.insteadOf" "git@github.com:"
  git config --global --add "url.${rewrite}.insteadOf" "ssh://git@github.com/"
  git config --global --add "url.${rewrite}.insteadOf" "git+ssh://git@github.com/"
  # uv caches failed clones — wipe so the next sync retries with the new auth.
  rm -rf "$HOME/.cache/uv/git-v0" 2>/dev/null || true
fi

# 3. Create venv + install dependencies from pyproject + uv.lock
cd "$INSTALL_DIR"
echo "==> uv sync"
uv sync

# 4. Persist config so subsequent invocations don't need to prompt
ENV_FILE="$INSTALL_DIR/.env"
{
  echo "PREFECT_API_URL=$PREFECT_API_URL"
  echo "WORK_POOL=$WORK_POOL"
  [[ -n "${WORK_QUEUE:-}" ]] && echo "WORK_QUEUE=$WORK_QUEUE"
  echo "WORKER_LIMIT=$WORKER_LIMIT"
  [[ -n "${GITHUB_TOKEN:-}" ]] && echo "GITHUB_TOKEN=$GITHUB_TOKEN"
} > "$ENV_FILE"
chmod 600 "$ENV_FILE"
echo "==> Wrote $ENV_FILE"

# 5. Start the worker inside a tmux session
if ! command -v tmux >/dev/null 2>&1; then
  echo "ERROR: tmux is not installed. Install it (e.g. apt install tmux) and re-run." >&2
  exit 1
fi

export PREFECT_API_URL
WORKER_ARGS=(--pool "$WORK_POOL" --limit "$WORKER_LIMIT")
if [[ -n "${WORK_QUEUE:-}" ]]; then
  WORKER_ARGS+=(--work-queue "$WORK_QUEUE")
fi

SESSION="prefect-worker-$(date +%Y%m%d-%H%M%S)"
echo "==> Starting tmux session: $SESSION"
echo "    cmd: prefect worker start ${WORKER_ARGS[*]}"

# `; read` keeps the pane open after the worker exits so you can see the error.
tmux new-session -d -s "$SESSION" -c "$INSTALL_DIR" \
  "PREFECT_API_URL='$PREFECT_API_URL' uv run prefect worker start ${WORKER_ARGS[*]@Q}; echo; echo '[worker exited — press enter to close]'; read"

cat <<EOF
==> Worker launched in tmux session '$SESSION'.

  Attach:    tmux attach -t $SESSION
  List:      tmux ls
  Stop:      tmux kill-session -t $SESSION
EOF
