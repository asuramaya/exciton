#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-claudeinator}"
REMOTE_ROOT="${REMOTE_ROOT:-/opt/photon/runtime}"

rsync -avz \
  --delete \
  --exclude '.git' \
  --exclude 'target' \
  --exclude 'photon.db' \
  --exclude 'photon.db-*' \
  ./ "${REMOTE_HOST}:${REMOTE_ROOT}/"

ssh "${REMOTE_HOST}" "cd ${REMOTE_ROOT} && docker compose build photon && docker compose up -d photon"
