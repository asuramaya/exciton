#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git rsync

if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi

mkdir -p /opt/photon/state
mkdir -p /opt/photon/runtime
mkdir -p /opt/photon/ssh
mkdir -p /srv/MadApes.ai

chmod 700 /opt/photon/ssh

if ! git -C /srv/MadApes.ai rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git init /srv/MadApes.ai >/dev/null
fi

echo "bootstrap complete"
