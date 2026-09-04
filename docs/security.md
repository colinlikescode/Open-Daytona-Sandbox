# Security model

What a sandbox can and cannot do, and what you are trusting.

## Isolation

- Every sandbox is a gVisor (`runsc`) container. Sandboxed code talks to gVisor's
  user-space kernel (the Sentry), not the host kernel. The host kernel sees one
  heavily seccomp-restricted process per sandbox.
- Workers refuse to start if Docker does not have a working `runsc` runtime. There is
  no silent fallback to `runc`. The dev-only `--unsafe-runc` worker flag exists for
  laptops without gVisor and prints a warning on every start.
- Containers run with all capabilities dropped except the few needed to
  `chown`/`setuid` inside the sandbox, `no-new-privileges`, a private IPC namespace,
  a pids limit, and CPU/memory cgroups. No host mounts. No privileged mode.
- gVisor defaults are kept: `systrap` platform, `directfs`, self-backed rootfs
  overlay (writes never reach the image layers), netstack networking.

## Network

- Sandboxes attach to a dedicated Docker bridge. iptables on that bridge drops
  traffic to `169.254.0.0/16` (cloud metadata), `10.0.0.0/8`, `172.16.0.0/12`,
  `192.168.0.0/16`, `100.64.0.0/10` and IPv6 link-local. A sandbox cannot reach the
  VM's instance credentials or anything else in your VPC. Return traffic for
  connections the sandbox opened is allowed.
- `network: none` gives a sandbox no interface at all.
- The worker API listens on the VM's loopback only. The control plane reaches it via
  `ssh -L`, using the SkyPilot-managed key. No security-group changes beyond SSH.
- Exposed ports (`get_url`) go through the control plane's signed-URL proxy:
  HMAC-SHA256 over sandbox id + port + expiry, verified on every request and on
  WebSocket upgrade. Tampering with the port or the sandbox id invalidates the token.

## Authentication

- Control plane on loopback: no token by default (only your user can reach it).
  Binding to anything else requires `api.token`; every route except `/v1/health`
  then demands `Authorization: Bearer <token>`, compared in constant time.
- Each worker gets a random 256-bit token at provision time, delivered in the
  root-only `/etc/sandboxpilot/worker.env`. The control plane sends it on every
  request. Tokens never appear in `sandboxpilot worker get` output or logs.
- Sandbox env values are write-only through the API: `SandboxInfo` exposes `env_keys`,
  never values. Logs redact `Bearer ...` and `?token=` query strings.

## What you are trusting

- gVisor's isolation. It is what Google uses for App Engine and Cloud Run.
- Your cloud IAM. SkyPilot launches VMs with the credentials on your machine; the
  VMs themselves need no cloud permissions (and the sandboxes cannot reach the
  metadata endpoint to borrow any).
- The host you run the control plane on. It holds the SSH key and the SQLite state.

## Not covered

- Side channels between sandboxes on the same VM (shared CPU cache, etc.). Use one
  pool per trust boundary if that matters to you.
- Sandbox egress to the public internet. Allowed by default; use `network: none` or
  add egress rules to the VM.
- Data at rest on the worker's disk. Workers are disposable; sandboxes' overlay data
  lives in `/var/lib/docker` on the VM until the VM is terminated.

Found something? Open an issue marked `security` or email the maintainers before
posting details publicly.
