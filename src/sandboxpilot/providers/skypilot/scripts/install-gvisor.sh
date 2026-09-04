#!/usr/bin/env bash
# Install gVisor (runsc) and register it as a Docker runtime.
# Supported: Ubuntu LTS and Debian on x86_64/arm64. Must run with sudo privileges.
set -euo pipefail

log() { echo "[sandboxpilot/gvisor] $*"; }

if command -v runsc >/dev/null 2>&1 && sudo docker info 2>/dev/null | grep -q 'runsc'; then
  log "runsc already installed and configured: $(runsc --version | head -n1)"
  exit 0
fi

. /etc/os-release
case "${ID:-}" in
  ubuntu|debian) ;;
  *) log "unsupported distribution '${ID:-unknown}'; only Ubuntu and Debian are supported"; exit 1 ;;
esac

ARCH="$(dpkg --print-architecture)"
case "$ARCH" in
  amd64|arm64) ;;
  *) log "unsupported architecture '$ARCH'"; exit 1 ;;
esac

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq curl gnupg ca-certificates

sudo install -m 0755 -d /usr/share/keyrings
curl -fsSL https://gvisor.dev/archive.key | sudo gpg --dearmor --yes -o /usr/share/keyrings/gvisor-archive-keyring.gpg
echo "deb [arch=${ARCH} signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" \
  | sudo tee /etc/apt/sources.list.d/gvisor.list >/dev/null
sudo apt-get update -qq
sudo apt-get install -y -qq runsc

# Registers "runsc" in /etc/docker/daemon.json (merges with existing config).
sudo runsc install
sudo systemctl restart docker

for _ in $(seq 1 30); do
  if sudo docker info 2>/dev/null | grep -q 'runsc'; then
    log "runsc configured: $(runsc --version | head -n1)"
    exit 0
  fi
  sleep 1
done

log "Docker did not report the runsc runtime after restart"
exit 1
