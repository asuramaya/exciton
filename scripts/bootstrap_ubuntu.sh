#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git rsync

if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi

EXCITON_ROOT="${EXCITON_ROOT:-/opt/exciton}"
PUBLISHER_REPO_DIR="${PUBLISHER_REPO_DIR:-/srv/publisher-target}"

mkdir -p "$EXCITON_ROOT/state"
mkdir -p "$EXCITON_ROOT/runtime"
mkdir -p "$EXCITON_ROOT/ssh"
mkdir -p "$PUBLISHER_REPO_DIR"

chmod 700 "$EXCITON_ROOT/ssh"

if ! git -C "$PUBLISHER_REPO_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git init "$PUBLISHER_REPO_DIR" >/dev/null
fi

echo "bootstrap complete"
