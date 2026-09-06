# Architecture

Three pieces. Two of them run on your machine.

```
 your machine                                        your cloud account
 ┌────────────────────────────────┐                  ┌──────────────────────────────────┐
 │ CLI / Python SDK / TS SDK      │                  │ worker VM (SkyPilot cluster)     │
 │        │ HTTP 127.0.0.1:7070   │                  │  sandboxpilot-worker :8080       │
 │        ▼                       │   ssh -L tunnel  │   │ Docker Engine API            │
 │ control plane (FastAPI)  ──────┼─────────────────▶│   ▼                              │
 │  scheduler · reconciler        │                  │  runsc  runsc  runsc  (warm slot)│
 │  sqlite state · templates      │                  └──────────────────────────────────┘
 └────────────────────────────────┘                   ... more workers per pool
```

## Control plane (`sandboxpilot.control`, `sandboxpilot.api`)

One process, started on demand by the SDK/CLI (`sandboxpilot daemon start`). Binds to
loopback by default; binding anywhere else requires an API token.

- `control/service.py` – the `ControlPlane`. Owns pools, workers, sandboxes, operations.
  Every public method is one API call.
- `scheduler/` – pure functions. `binpack.select_worker` picks the tightest fit,
  `scaler.py` decides scale up/down, `capacity.py` tracks reservations so two concurrent
  creates never overbook a worker.
- `control/tunnels.py` – one `ssh -N -L` per worker. The worker API never has a public
  port. `FakeTunnelManager` is used in tests.
- `control/worker_client.py` – typed HTTP client for the worker API, with per-worker
  connection pooling.
- `state/` – SQLite through aiosqlite. Numbered migrations in `state/migrations/`.
  Repositories are thin; no ORM.
- `api/routes/` – one module per resource. Errors are `SandboxPilotError` subclasses
  and always serialize to `{"error": {"code", "message", "hint"}}`.

On startup the control plane reconciles: workers that were mid-provision are
terminated, sandboxes that were mid-create are checked against their worker, dead
tunnels are reopened. Losing the control plane process never loses a sandbox.

## Provider (`sandboxpilot.providers`)

`ComputeProvider` has three real methods: `provision`, `terminate`, `list_clusters`.
The only production implementation is `SkyPilotComputeProvider`. AWS, GCP and Azure
are never touched directly; `tests/unit/test_skypilot_only.py` fails the build if a
cloud SDK import shows up.

Provisioning = `sky.launch` of a task whose `setup` runs `bootstrap-worker.sh`: install
Docker + gVisor, register `runsc` as a Docker runtime, install the `sandboxpilot`
package (from PyPI or a wheel of your checkout), write `/etc/sandboxpilot/worker.env`,
enable the systemd unit, and verify `docker run --runtime=runsc` actually works. If
that check fails the worker never reports ready. Since SkyPilot 0.9 `launch` returns
as soon as the job is *submitted*, so the provider then follows the job (`tail_logs`,
falling back to `job_status` polling) until the bootstrap has finished before it opens
the tunnel. Measured on GCP (`n4-standard-8`, Ubuntu 22.04): about 5 minutes from
request to HEALTHY, most of it apt + gVisor + pip.

`FakeComputeProvider` runs real `WorkerService` instances in-process. Everything in
`tests/` except the `gvisor`/`docker`/`cloud_*` markers runs against it.

## Worker (`sandboxpilot.worker`)

A FastAPI app on the VM, reachable only through the tunnel, authenticated with a
per-worker bearer token.

- `service.py` – `WorkerService`: create/kill sandboxes, run commands, files, reaper,
  warm slots, capacity accounting, crash-safe state in `/var/lib/sandboxpilot`.
- `runtime/base.py` – `SandboxRuntime` interface. `docker_gvisor.py` is the real one
  (Docker Engine API + `runsc`). `fake.py` is an in-memory implementation with a tiny
  shell, used by tests and the fake provider.
- `commands.py` – bounded output buffers, streaming, timeouts, kill.
- `firewall.py` – iptables rules on the sandbox bridge: drop 169.254.0.0/16, RFC1918
  and 100.64.0.0/10 so a sandbox cannot reach cloud metadata or anything else in your
  VPC, plus an INPUT chain so it cannot reach the worker VM itself.

Two gVisor facts shape the runtime (`runtime/docker_gvisor.py`):

- gVisor's netstack owns the sandbox's loopback, so Docker's embedded DNS at
  `127.0.0.11` is unreachable. Every sandbox gets a read-only `/etc/resolv.conf`
  naming public resolvers instead.
- gVisor caches the rootfs and keeps the sandbox's writes in its own overlay, so
  `docker cp` is unreliable in both directions once a sandbox is running. File
  transfer streams a tar into `tar -x` (upload) and out of `tar -c` (download) executed
  *inside* the sandbox; `docker cp` is only a fallback for images without `tar`.

## Warm slots

A cold `runsc` boot is 100-300 ms on cloud VMs (mostly Sentry init, worse on
high-core-count instances). Workers therefore keep `warm_slots` sandboxes pre-booted
with the pool's default spec. A create request whose image/network/user/workdir match
claims one: the container is renamed, its cgroup limits are updated live to the
requested CPU/memory/pids, env is injected per exec. The pool refills in the
background. Non-matching requests take the cold path, evicting a warm slot if one is
holding the capacity they need. Reported capacity excludes warm slots, so the
scheduler treats them as free space.

## Request path for `Sandbox.create()`

1. SDK checks `GET /v1/health`; starts the daemon if nothing answers on loopback.
2. Control plane merges template + pool defaults into a `SandboxSpec`, inserts a
   `PENDING` record, and asks the scheduler for a worker.
3. No fit → scale decision. Scale-up provisions a worker through SkyPilot (minutes);
   the create waits up to `create_timeout` for capacity.
4. Fit → `POST /v1/sandboxes` on the worker through the tunnel. The worker claims a
   warm slot or cold-creates a `runsc` container, verifies it can exec, returns.
5. Record moves to `RUNNING`. Commands, files and proxy requests are forwarded to the
   same worker for the sandbox's lifetime.

## Where to start reading

New engineer? `control/service.py` (`create_sandbox`), then `worker/service.py`
(`_create`, `_claim_warm`), then `providers/skypilot/provider.py` (`provision`).
