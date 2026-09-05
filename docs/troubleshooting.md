# Troubleshooting

Start with:

```bash
sandboxpilot doctor            # control plane, state, SkyPilot, clouds, ssh, pools, workers
sandboxpilot daemon status     # is the local control plane up, pid, log path
sandboxpilot daemon logs -n 200
sandboxpilot --json status     # everything the control plane knows, including per-worker capacity
```

## The control plane does not start

`The control plane did not become healthy at http://127.0.0.1:7070 within 20s`

- Read `sandboxpilot daemon logs`. Typical causes: a broken `config.yaml`
  (`Invalid configuration: ...`), the port in use, or a state database written by a
  newer SandboxPilot (`Upgrade SandboxPilot`).
- `SANDBOXPILOT_API_URL` pointing at a remote host disables autostart: start the
  control plane there, or unset the variable.
- Binding to anything other than loopback requires `api.token`.

## Worker provisioning fails

Provisioning is a SkyPilot launch plus a bootstrap script. `sandboxpilot pool up`
prints the failure; `sandboxpilot --json status` shows `last_error` per worker.

| message | fix |
|---|---|
| `credentials for ... are unavailable` | `sky check`; configure the cloud CLI ([clouds](clouds.md)) |
| `no available capacity on ... for the requested worker size` | another region (`pool create --region`), a smaller worker, or another cloud |
| `cloud quota limit` | request a quota increase or use a different instance type/cloud |
| `Worker bootstrap on the cloud VM failed or timed out` | `sky logs sp-<pool>-<id>` shows the bootstrap output; see below |
| `worker protocol version ... incompatible` | the VM installed a different SandboxPilot; reprovision after upgrading both sides |

Bootstrap requirements on the VM: Ubuntu LTS or Debian, systemd, passwordless sudo,
outbound internet. The script fails (and the worker is terminated) if Docker does not
expose the `runsc` runtime after installing gVisor, or if the worker's own gVisor
smoke test fails. Running from a source checkout uploads a wheel of your checkout
(`SANDBOXPILOT_WORKER_INSTALL=local`); a release install pulls `sandboxpilot[worker]`
from PyPI (`=release`).

Leftover VMs: `sky status` lists every cluster; `sky down <name>` removes one;
`sandboxpilot cleanup --workers` terminates everything the control plane knows about.

## Worker is UNHEALTHY or LOST

- `UNHEALTHY`: health checks fail but SkyPilot still reports the VM up. The tunnel is
  re-opened on every check; try `ssh sp-<pool>-<id>` (SkyPilot's alias) and
  `sudo journalctl -u sandboxpilot-worker` on the VM.
- `LOST`: the VM disappeared (spot preemption, manual termination). Its sandboxes are
  `LOST`; a replacement is provisioned if the pool is below `min_workers`.
- Workers that were mid-provision when the control plane restarted are terminated on
  startup unless SkyPilot reports them up, in which case they are re-adopted.

## Sandbox problems

| symptom | cause |
|---|---|
| `no_capacity: ... can never fit on a ... worker` | request exceeds a whole worker after reserves; smaller sandbox or bigger `workers` |
| `create_timeout` | at `max_workers` with no free capacity, or provisioning slower than `create_timeout` |
| `sandbox_lost` | worker gone; create a new sandbox |
| state `EXPIRED` | hit its `timeout`; use `sb.set_timeout()` or a longer default |
| `Sandbox readiness check failed ... must provide /bin/sh` | shell-less image; pass `keepalive_command` and use `args=[...]` for commands |
| `Image pull failed` | wrong reference, or private registry: configure Docker credentials on the workers |
| `Sandbox process was terminated after exceeding its ... memory limit` | OOM; raise `memory` |
| command `TIMED_OUT` | `timeout` elapsed; the process tree was killed |

Command output is truncated to `limits.max_command_output_bytes` (head and tail
kept). Stream (`sb.stream`) to see everything.

## Proxy URLs

`get_url` returns `http://<api>/v1/proxy/<sandbox>/<port>/<token>/`. A `401` means the
token expired (`limits.proxy_url_ttl_seconds`) or the URL was altered; `502` means
nothing listens on that port inside the sandbox (or `network: none`). If clients
cannot reach `127.0.0.1:7070`, set `api.external_url`.

## Running a worker by hand (Linux, Docker + gVisor)

```bash
pip install "sandboxpilot[worker]"
sudo runsc install && sudo systemctl restart docker
export SANDBOXPILOT_WORKER_TOKEN=$(python -c 'import secrets;print(secrets.token_urlsafe(32))')
sudo -E python -m sandboxpilot.worker.setup --network --firewall --doctor
sudo -E sandboxpilot-worker            # listens on 127.0.0.1:9417
```

`pytest -m gvisor` runs the real-runtime tests against it. Without gVisor,
`SANDBOXPILOT_WORKER_RUNTIME=docker-unsafe SANDBOXPILOT_DEV_UNSAFE_RUNTIME=1` uses
plain `runc` for development only; it provides no isolation.

## Resetting

`sandboxpilot daemon stop`, then delete the state directory
(`~/.local/share/sandboxpilot`, or `SANDBOXPILOT_STATE_DIR`). Workers keep running:
terminate them with `sky down <cluster>` first if you want the bill to stop.
