# Cloud setup (AWS, GCP, Azure)

SandboxPilot never calls a cloud API itself. SkyPilot launches, monitors and
terminates the worker VMs using the credentials already on your machine. Setting up a
cloud therefore means setting it up for SkyPilot.

```bash
pip install "sandboxpilot[aws]"      # or [gcp], [azure], or [aws,gcp,azure]
sky check                            # SkyPilot's own credential report
sandboxpilot doctor                  # same information, plus ssh/state/worker checks
```

## AWS

- `aws configure` (or `AWS_PROFILE` / instance credentials). The identity needs to
  create and terminate EC2 instances, security groups and key pairs in the region.
- Default worker: `8+` vCPU, `32+` GB (SkyPilot picks e.g. `c7i.4xlarge`/`m6i.2xlarge`).
- Spot: `workers.spot: true` (pool config) or `sandboxpilot pool create ... --spot`.

## GCP

- `gcloud auth application-default login` and `gcloud config set project <id>`.
- Enable the Compute Engine API once. SkyPilot creates the firewall rule for SSH.

## Azure

- `az login` and `az account set -s <subscription>`.
- `pip install "sandboxpilot[azure]"` pulls `azure-cli` through SkyPilot; expect a
  large install.

## Choosing a cloud

```yaml
pools:
  default:
    cloud:
      providers: auto        # aws, gcp, azure, or a list, or auto (all three)
      strategy: cost         # cost | time
      region: null           # requires exactly one provider
      zone: null
      instance_type: null    # pins CPU/memory; overrides workers.cpus/memory
```

With `auto`, SkyPilot's optimizer evaluates every enabled cloud and launches the
cheapest instance that satisfies the worker size (`strategy: time` prefers the
fastest to provision). Pin a cloud with `sandboxpilot up --cloud aws` or
`sandboxpilot pool create gpu --cloud gcp --region us-central1`.

## What runs in your account

- One VM per worker, named `sp-<pool>-<worker-id>`, tagged
  `sandboxpilot-managed=true`, `sandboxpilot-pool-id`, `sandboxpilot-worker-id`.
- The VM needs outbound internet during bootstrap (apt, gVisor release repo, PyPI or
  the wheel SkyPilot uploads) and to pull sandbox images.
- No inbound ports beyond SSH. The worker API is reached through `ssh -L`.
- The VM has no cloud IAM role requirement; sandboxes cannot reach the metadata
  endpoint anyway (see [security](security.md)).

`sandboxpilot down` (or `pool down`) runs `sky down` on every worker. If the control
plane is gone, `sky status` and `sky down <cluster>` still work, because SkyPilot
keeps its own record of the clusters.

## Costs

`sandboxpilot status` shows the hourly price SkyPilot reported for each worker. You
pay the VM price only. Idle workers are terminated after `scaling.idle_ttl` (default
15 minutes) down to `scaling.min_workers` (default 0).

## Local development without a cloud

```bash
sandboxpilot init --cloud fake
sandboxpilot run "echo hi"
```

The `fake` provider boots in-process workers with an in-memory runtime. Real gVisor
without a cloud: run `sandboxpilot-worker` on a Linux box with Docker + `runsc`
(see [troubleshooting](troubleshooting.md)).
