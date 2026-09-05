# Pools, sizing and scaling

A pool is one cloud policy plus one worker shape plus autoscaling rules. Sandboxes
are created in a pool (`default` unless you say otherwise) and packed onto its
workers.

## Configuration

`~/.config/sandboxpilot/config.yaml` (or `SANDBOXPILOT_CONFIG`). Every key is optional.

```yaml
api:
  host: 127.0.0.1          # anything else requires api.token
  port: 7070
  token: null
  external_url: null       # base URL clients should use for proxy URLs

defaults:
  pool: default
  sandbox_timeout: null    # overrides every pool's sandbox.timeout when set (also SANDBOXPILOT_SANDBOX_TIMEOUT)
  create_timeout: 900      # seconds a create may wait for capacity

limits:
  max_command_output_bytes: 4MB
  max_upload_size: 512MB
  max_download_size: 512MB
  proxy_url_ttl_seconds: 3600
  idempotency_ttl_seconds: 86400

reconcile:
  interval_seconds: 10
  health_failures_before_lost: 6
  worker_provision_timeout_seconds: 1800
  worker_health_timeout_seconds: 5

pools:
  default:
    cloud: { providers: auto, strategy: cost }
    workers:
      cpus: 8
      memory: 32GB
      disk: 100GB
      spot: false
      reserve: { cpus: 1, memory_bytes: 2GB }   # kept for the OS, Docker and the worker
      max_sandboxes: null
      warm_slots: 2
    scaling:
      min_workers: 0
      max_workers: 4
      idle_ttl: 15m
    runtime: { type: gvisor }                  # gvisor | fake | docker-unsafe (dev only)
    sandbox:
      image: python:3.12-slim
      cpus: 1
      memory: 2GB
      pids_limit: 1024
      network: internet                        # internet | none
      timeout: 1h
      max_timeout: 24h
      workdir: /workspace
    images:
      preload: []                              # pulled onto every worker at bootstrap
```

Precedence for a sandbox's settings: request > template > `defaults.sandbox_timeout`
(timeout only) > pool `sandbox` defaults. Pools defined in the file are upserted into
the state database at start; pools created with the CLI/API live only in the database.

## Sizing

- Allocatable capacity per worker = `workers.cpus - reserve.cpus`,
  `workers.memory - reserve.memory`. A request larger than that is rejected
  immediately with `no_capacity`.
- The scheduler bin-packs: the tightest-fitting healthy worker wins, so idle workers
  drain and get terminated. `workers.max_sandboxes` caps the count per worker.
- Warm slots (`workers.warm_slots`) are pre-booted sandboxes with the pool's default
  spec. A create whose image/network/user/workdir match claims one in ~20 ms instead
  of a 100-300 ms cold boot; CPU/memory/pids are adjusted live. They do not count as
  used capacity and are evicted when a non-matching request needs the room. Changing
  `warm_slots` or `sandbox` defaults on a pool applies to workers provisioned after
  the change.

## Scaling

- Scale up happens when no healthy worker fits a request and the pool is below
  `max_workers`. While a worker is provisioning, further requests wait for it rather
  than launching more VMs. A create waits up to `create_timeout` in total.
- At `max_workers` requests queue (`WAITING_FOR_CAPACITY`) until a sandbox is killed
  or expires. Killing a queued sandbox cancels the create.
- Scale down terminates workers with no sandboxes for longer than `idle_ttl`, oldest
  first, never below `min_workers`. `sandboxpilot worker drain <id>` stops scheduling
  onto a worker and terminates it once empty.
- `min_workers > 0` keeps that many workers warm; the reconciler replaces lost ones.
- `sandboxpilot up --workers N` provisions N workers ahead of demand;
  `sandboxpilot down` terminates everything in the pool.

## Health and recovery

Every `reconcile.interval_seconds` the control plane health-checks each worker,
compares its sandbox inventory with the worker's, kills expired sandboxes, tops up
`min_workers` and scales down idle workers. A worker that fails
`health_failures_before_lost` checks and that SkyPilot reports as gone is marked
`LOST` (its sandboxes become `LOST` too); one SkyPilot still reports as up stays
`UNHEALTHY` and is retried. The control plane can be restarted at any time: workers
and running sandboxes are re-adopted from SQLite and the workers' own state.

## Multiple pools

Use separate pools for separate trust boundaries, instance shapes or clouds:

```bash
sandboxpilot pool create builds --cloud aws --cpus 16 --memory 64GB --max-workers 8 --spot
sandboxpilot pool create eu --cloud azure --region westeurope --image node:22-slim
sandboxpilot create --pool builds --cpus 4 --memory 8GB
```
