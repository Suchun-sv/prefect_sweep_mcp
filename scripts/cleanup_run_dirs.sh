#!/usr/bin/env bash
# Delete prefect-sweep per-run worker directories older than RUNS_TTL_DAYS.
# Used as a daily cron job by install_worker.sh.
#
# Layout: each flow run clones into <repo_local_path>/.runs/<flow_run_id>/.
# Successful runs remove their dir at flow end, but failed/interrupted runs
# leave the dir behind for post-mortem.
#
# Env:
#   RUNS_CLEANUP_ROOT  default: $HOME/github
#   RUNS_TTL_DAYS      default: 1
set -u
ROOT="${RUNS_CLEANUP_ROOT:-$HOME/github}"
DAYS="${RUNS_TTL_DAYS:-1}"
[[ -d "$ROOT" ]] || exit 0

# Match exact `*/.runs/<id>` directories, prune (don't recurse into a dir
# we are about to delete), and only act on those older than $DAYS days.
find "$ROOT" -mindepth 2 -maxdepth 6 -type d -path '*/.runs/*' \
  -mtime "+$DAYS" -prune -exec rm -rf {} + 2>/dev/null || true
