#!/usr/bin/env bash
set -euo pipefail

# Required: REMOTE_HOST=ssh-target ./scripts/deploy_remote.sh
REMOTE_HOST="${REMOTE_HOST:?set REMOTE_HOST to your ssh target}"
REMOTE_ROOT="${REMOTE_ROOT:-/opt/exciton/runtime}"

rsync -avz \
  --delete \
  --exclude '.git' \
  --exclude 'target' \
  --exclude 'exciton.db' \
  --exclude 'exciton.db-*' \
  ./ "${REMOTE_HOST}:${REMOTE_ROOT}/"

ssh "${REMOTE_HOST}" "cd ${REMOTE_ROOT} && docker compose build exciton && docker compose up -d exciton"
