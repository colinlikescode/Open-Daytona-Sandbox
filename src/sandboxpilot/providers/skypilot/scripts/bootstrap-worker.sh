#!/usr/bin/env bash
# SandboxPilot worker bootstrap.
#
# Turns a freshly provisioned Linux VM into a SandboxPilot worker:
#   1. verify supported host          8. configure sandbox network + firewall
#   2. install Docker                 9. install systemd service
#   3. install gVisor (runsc)        10. start service
#   4. configure Docker runtime      11. verify worker health
#   5. install SandboxPilot worker   12. verify gVisor smoke test (done by the worker doctor)
#   6. configure worker env          13. preload images (done by the worker on start)
#   7. (Docker restart handled by install-gvisor.sh)
#
# Any failure exits non-zero, which fails SkyPilot setup, which fails provisioning.
# The worker is never reported READY unless every mandatory step succeeded.
#
# Inputs (environment):
#   SP_STAGE_DIR      directory with staged files (default /tmp/sandboxpilot)
#   SP_INSTALL_MODE   release | local
#   SP_VERSION        SandboxPilot version to install (release mode)
#   SP_PIP_INDEX_URL  optional extra index
set -euo pipefail

STAGE="${SP_STAGE_DIR:-/tmp/sandboxpilot}"
MODE="${SP_INSTALL_MODE:-release}"
VERSION="${SP_VERSION:-}"
VENV=/opt/sandboxpilot
ENV_DIR=/etc/sandboxpilot
ENV_FILE="$ENV_DIR/worker.env"
STATE_DIR=/var/lib/sandboxpilot
UNIT=/etc/systemd/system/sandboxpilot-worker.service

log() { echo "[sandboxpilot/bootstrap] $*"; }
die() { log "ERROR: $*"; exit 1; }

# 1. Supported host -----------------------------------------------------------
[ "$(uname -s)" = "Linux" ] || die "worker hosts must run Linux"
command -v systemctl >/dev/null 2>&1 || die "systemd is required"
[ -r /etc/os-release ] || die "cannot detect distribution"
. /etc/os-release
case "${ID:-}" in
  ubuntu|debian) ;;
  *) die "unsupported distribution '${ID:-unknown}' (supported: Ubuntu LTS, Debian)" ;;
esac
sudo -n true 2>/dev/null || die "passwordless sudo is required during bootstrap"
[ -f "$STAGE/worker.env" ] || die "staged worker.env not found in $STAGE"

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl iptables python3 python3-venv python3-pip

# 2. Docker -------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  log "installing Docker"
  sudo apt-get install -y -qq docker.io
fi
sudo systemctl enable --now docker
sudo usermod -aG docker "${USER:-$(id -un)}" || true
for _ in $(seq 1 30); do sudo docker info >/dev/null 2>&1 && break; sleep 1; done
sudo docker info >/dev/null 2>&1 || die "Docker daemon is not responding"

# 3/4. gVisor -----------------------------------------------------------------
bash "$STAGE/install-gvisor.sh"
sudo docker info 2>/dev/null | grep -q runsc || die "Docker does not expose the runsc runtime"

# 5. SandboxPilot worker package ---------------------------------------------
# The worker needs Python >= 3.11. Ubuntu 22.04 (SkyPilot's default image) ships
# 3.10, so when the system interpreter is too old we let uv fetch a managed one.
MIN_PY="3.11"
py_ok() { "$1" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; }
if [ ! -x "$VENV/bin/python" ] || ! py_ok "$VENV/bin/python"; then
  sudo rm -rf "$VENV"
  if py_ok python3; then
    log "using system $(python3 --version)"
    sudo python3 -m venv "$VENV"
  else
    log "system python is older than $MIN_PY; installing a managed interpreter with uv"
    if [ ! -x /usr/local/bin/uv ]; then
      curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
    fi
    sudo env UV_PYTHON_INSTALL_DIR=/opt/sandboxpilot-python /usr/local/bin/uv venv --python 3.12 --seed "$VENV"
  fi
fi
py_ok "$VENV/bin/python" || die "could not get a Python >= $MIN_PY interpreter"
sudo "$VENV/bin/python" -m pip install -q --upgrade pip wheel
if [ "$MODE" = "local" ]; then
  WHEEL="$(ls "$STAGE"/wheels/sandboxpilot-*.whl | head -n1)"
  [ -n "$WHEEL" ] || die "no wheel found in $STAGE/wheels"
  log "installing local wheel $WHEEL"
  sudo "$VENV/bin/python" -m pip install -q --force-reinstall "${WHEEL}[worker]"
else
  [ -n "$VERSION" ] || die "SP_VERSION is required in release mode"
  log "installing sandboxpilot[worker]==$VERSION"
  sudo "$VENV/bin/python" -m pip install -q ${SP_PIP_INDEX_URL:+--extra-index-url "$SP_PIP_INDEX_URL"} "sandboxpilot[worker]==$VERSION"
fi
"$VENV/bin/python" -c 'import sandboxpilot.worker.app' || die "worker package did not import cleanly"

# 6. Worker configuration (root-only) -----------------------------------------
sudo install -d -m 0700 "$ENV_DIR" "$STATE_DIR"
sudo install -m 0600 "$STAGE/worker.env" "$ENV_FILE"
shred -u "$STAGE/worker.env" 2>/dev/null || rm -f "$STAGE/worker.env"

# 7. Host tuning for gVisor boot latency ----------------------------------------
# Transparent huge pages: `enabled=always` + `defrag=defer` gives the Sentry 2 MB
# pages without blocking page faults (measured ~10 ms off every cold boot on cloud
# VMs). shmem_enabled=advise lets gVisor's MemoryFile take huge pages too. Idempotent.
sudo tee /etc/tmpfiles.d/sandboxpilot-thp.conf >/dev/null <<'EOF'
w /sys/kernel/mm/transparent_hugepage/enabled - - - - always
w /sys/kernel/mm/transparent_hugepage/defrag - - - - defer
w /sys/kernel/mm/transparent_hugepage/shmem_enabled - - - - advise
EOF
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sandboxpilot-thp.conf 2>/dev/null || true
# Sandboxes are memory-limited; do not let the host swap their pages out.
sudo sysctl -q -w vm.swappiness=10 && echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/90-sandboxpilot.conf >/dev/null

# 8. Sandbox network + firewall -----------------------------------------------
sudo env -i PATH="$PATH" bash -c "set -a; . $ENV_FILE; set +a; $VENV/bin/python -m sandboxpilot.worker.setup --network --firewall"

# 9/10. systemd service ----------------------------------------------------------
sudo tee "$UNIT" >/dev/null <<EOF
[Unit]
Description=SandboxPilot worker daemon
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$ENV_FILE
ExecStartPre=$VENV/bin/python -m sandboxpilot.worker.setup --network --firewall
ExecStart=$VENV/bin/sandboxpilot-worker
Restart=on-failure
RestartSec=3
StartLimitIntervalSec=300
StartLimitBurst=10
User=root
NoNewPrivileges=false
ProtectHome=yes
PrivateTmp=no

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable sandboxpilot-worker.service
sudo systemctl restart sandboxpilot-worker.service

# 11. Health verification (includes the worker's gVisor smoke test) ------------
TOKEN="$(sudo sed -n 's/^SANDBOXPILOT_WORKER_TOKEN=//p' "$ENV_FILE")"
PORT="$(sudo sed -n 's/^SANDBOXPILOT_WORKER_PORT=//p' "$ENV_FILE")"
PORT="${PORT:-9417}"
for i in $(seq 1 120); do
  if out="$(curl -fsS -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:${PORT}/v1/health" 2>/dev/null)"; then
    if echo "$out" | grep -q '"status":"healthy"'; then
      log "worker healthy: $(echo "$out" | head -c 300)"
      exit 0
    fi
  fi
  if ! sudo systemctl is-active --quiet sandboxpilot-worker.service && [ "$i" -gt 10 ]; then
    sudo journalctl -u sandboxpilot-worker.service --no-pager -n 50 || true
    die "sandboxpilot-worker service is not running"
  fi
  sleep 2
done
sudo journalctl -u sandboxpilot-worker.service --no-pager -n 50 || true
die "worker did not become healthy in time"
