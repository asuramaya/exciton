#!/usr/bin/env bash
# One-shot rename of a live photon deployment to exciton naming.
#
# What it does (idempotent — re-runnable safely):
#   1. Stops madapes-photon + madapes-zeroclaw containers.
#   2. Renames host directories:
#        ~/photon-runtime   →  ~/exciton-runtime
#        ~/photon-state     →  ~/exciton-state
#        ~/photon-ssh       →  ~/exciton-ssh
#   3. Renames the SQLite DB inside the state dir:
#        photon.db{,.wal,.shm} → exciton.db{,.wal,.shm}
#   4. Patches ~/madapes-stack/docker-compose.yml in place:
#        service:photon → service:exciton
#        container_name: madapes-photon → madapes-exciton
#        image: madapes-photon → madapes-exciton
#        PHOTON_* env vars → EXCITON_*
#        /data/photon.db → /data/exciton.db
#        volume paths to use exciton-* directories
#        /etc/photon/config.toml → /etc/exciton/config.toml
#        exec photon → exec exciton
#   5. Renames ~/.ssh/photon_deploy → ~/.ssh/exciton_deploy + the
#      `github-photon` Host alias in ~/.ssh/config → `github-exciton`.
#      photon-runtime's git remote URL is rewritten to use the new alias.
#   6. Rebuilds the image + brings the stack back up.
#
# What it deliberately leaves alone:
#   - madapes-zeroclaw container/image (zeroclaw rename is a separate
#     concern; the photon deployment is what's renaming).
#   - ~/MadApes.ai checkout (publisher target — naming is intentional).
#   - ~/madapes-stack directory (project name, not the engine name).
#
# Run with the operator account on the VM (e.g. claudeinator):
#   ssh claudeinator
#   ./scripts/migrate_vm_to_exciton.sh

set -euo pipefail

HOME_DIR="${HOME:-/home/claudeinator}"
STACK_DIR="${STACK_DIR:-$HOME_DIR/madapes-stack}"
COMPOSE_FILE="${COMPOSE_FILE:-$STACK_DIR/docker-compose.yml}"

step() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[ok]\033[0m %s\n' "$*"; }
skip() { printf '\033[1;30m[skip]\033[0m %s\n' "$*"; }

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "compose file not found at $COMPOSE_FILE — set COMPOSE_FILE to override"
  exit 1
fi

step "1. Stop containers"
if docker ps --format '{{.Names}}' | grep -qx 'madapes-photon\|madapes-exciton'; then
  docker compose -f "$COMPOSE_FILE" down
  ok "stack stopped"
else
  skip "stack already down"
fi

step "2. Rename host directories"
rename_dir() {
  local src="$1" dst="$2"
  if [ -e "$dst" ] && [ -e "$src" ]; then
    warn "both $src and $dst exist; merge manually before re-running"
    exit 1
  fi
  if [ -e "$src" ]; then
    mv -v "$src" "$dst"
    ok "$src → $dst"
  else
    skip "$src not present"
  fi
}
rename_dir "$HOME_DIR/photon-runtime" "$HOME_DIR/exciton-runtime"
rename_dir "$HOME_DIR/photon-state"   "$HOME_DIR/exciton-state"
rename_dir "$HOME_DIR/photon-ssh"     "$HOME_DIR/exciton-ssh"

step "3. Rename SQLite DB files"
STATE_DIR="$HOME_DIR/exciton-state"
for ext in '' '-wal' '-shm'; do
  src="$STATE_DIR/photon.db$ext"
  dst="$STATE_DIR/exciton.db$ext"
  if [ -e "$src" ]; then
    mv -v "$src" "$dst"
    ok "photon.db$ext → exciton.db$ext"
  else
    skip "photon.db$ext not present"
  fi
done

step "4. Patch docker-compose.yml"
cp -a "$COMPOSE_FILE" "$COMPOSE_FILE.bak.$(date +%Y%m%d-%H%M%S)"
sed -i \
  -e 's|/home/claudeinator/photon-runtime|/home/claudeinator/exciton-runtime|g' \
  -e 's|/home/claudeinator/photon-state|/home/claudeinator/exciton-state|g' \
  -e 's|/home/claudeinator/photon-ssh|/home/claudeinator/exciton-ssh|g' \
  -e 's|^  photon:$|  exciton:|g' \
  -e 's|container_name: madapes-photon|container_name: madapes-exciton|g' \
  -e 's|image: madapes-photon:latest|image: madapes-exciton:latest|g' \
  -e 's|PHOTON_DB_PATH: /data/photon\.db|EXCITON_DB_PATH: /data/exciton.db|g' \
  -e 's|PHOTON_DISABLE_MCP|EXCITON_DISABLE_MCP|g' \
  -e 's|PHOTON_MCP_PORT|EXCITON_MCP_PORT|g' \
  -e 's|PHOTON_MCP_TOKEN|EXCITON_MCP_TOKEN|g' \
  -e 's|/etc/photon/config\.toml|/etc/exciton/config.toml|g' \
  -e 's|exec photon |exec exciton |g' \
  -e 's|depends_on:\n      - photon|depends_on:\n      - exciton|g' \
  "$COMPOSE_FILE"
ok "compose patched (backup at $COMPOSE_FILE.bak.*)"

step "5. Rename SSH deploy key + config alias"
if [ -f "$HOME_DIR/.ssh/photon_deploy" ] && [ ! -f "$HOME_DIR/.ssh/exciton_deploy" ]; then
  mv -v "$HOME_DIR/.ssh/photon_deploy"     "$HOME_DIR/.ssh/exciton_deploy"
  if [ -f "$HOME_DIR/.ssh/photon_deploy.pub" ]; then
    mv -v "$HOME_DIR/.ssh/photon_deploy.pub" "$HOME_DIR/.ssh/exciton_deploy.pub"
  fi
  ok "ssh deploy key renamed"
else
  skip "ssh deploy key already migrated or absent"
fi
if grep -q '^Host github-photon' "$HOME_DIR/.ssh/config" 2>/dev/null; then
  sed -i \
    -e 's|^Host github-photon|Host github-exciton|' \
    -e 's|IdentityFile.*photon_deploy|IdentityFile ~/.ssh/exciton_deploy|' \
    "$HOME_DIR/.ssh/config"
  ok "ssh config alias renamed (github-photon → github-exciton)"
else
  skip "github-photon alias not present in ~/.ssh/config"
fi

if [ -d "$HOME_DIR/exciton-runtime/.git" ]; then
  remote=$(git -C "$HOME_DIR/exciton-runtime" remote get-url origin 2>/dev/null || echo "")
  if echo "$remote" | grep -q 'github-photon\|asuramaya/photon'; then
    new_remote=$(echo "$remote" \
      | sed -e 's|github-photon|github-exciton|' -e 's|asuramaya/photon|asuramaya/exciton|')
    git -C "$HOME_DIR/exciton-runtime" remote set-url origin "$new_remote"
    ok "git remote rewritten: $remote → $new_remote"
  else
    skip "git remote already points at exciton"
  fi
fi

step "6. Rebuild + bring stack up"
docker compose -f "$COMPOSE_FILE" build exciton
docker compose -f "$COMPOSE_FILE" up -d
ok "stack up; verify with: docker ps && docker logs madapes-exciton --since 1m"

printf '\n\033[1;32mMigration complete.\033[0m\n'
printf 'Old container/image madapes-photon will linger as a stopped artifact until\n'
printf 'you run: docker rm madapes-photon 2>/dev/null; docker rmi madapes-photon:latest\n'
