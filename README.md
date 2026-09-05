# Open-Daytona-Sandbox

Fast, isolated sandboxes for AI agents, in your own cloud account.

**What it is.** You bring an AWS, GCP or Azure account. SandboxPilot gives you an
E2B-style sandbox API on top of it. SkyPilot creates the VMs. SandboxPilot keeps them
warm and runs gVisor sandboxes on them.

**Why it exists.** E2B, Modal and Daytona all require you to ship your code, secrets
and the agent's outputs to their infrastructure. Every regulated buyer (healthcare,
finance, defense, EU companies under GDPR, anyone with a SOC 2 auditor) either can't
use them or spends months on vendor review. "Your VPC, your IAM, your audit logs, our
open-source software" ends that conversation in one sentence.

```
 your laptop / CI box                        your cloud account (AWS, GCP or Azure)
 ┌───────────────────────────┐               ┌──────────────────────────────────────┐
 │  Python SDK / TS SDK / CLI│               │  worker VM  (launched by SkyPilot)   │
 │            │              │   ssh tunnel  │  ┌─────────┐ ┌─────────┐ ┌────────┐  │
 │  sandboxpilot control     │══════════════▶│  │ sandbox │ │ sandbox │ │sandbox │  │
 │  plane  127.0.0.1:7070    │               │  │ (gVisor)│ │ (gVisor)│ │(gVisor)│  │
 │  state: one sqlite file   │               │  └─────────┘ └─────────┘ └────────┘  │
 └───────────────────────────┘               │  worker VM ...                       │
                                             └──────────────────────────────────────┘
```

- A sandbox starts in about a second on a warm worker. One VM hosts many sandboxes.
  You never wait for a VM to boot per sandbox.
- Every sandbox is a gVisor (`runsc`) container. No cloud metadata access, no private
  network access. Plain `runc` is never used silently.
- Nothing listens on the public internet. Workers are reached over SSH tunnels.
- You pay your cloud's VM price and nothing else. `sandboxpilot down` stops the bill.
- AWS, GCP and Azure all go through SkyPilot. Pin a cloud or let it pick the cheapest.

## 30 seconds

```bash
pip install "sandboxpilot[aws]"        # or [gcp], [azure], or [aws,gcp,azure]

sandboxpilot doctor                     # checks cloud credentials, SkyPilot, ssh
sandboxpilot up                         # starts one warm worker (~2-4 minutes, once)
sandboxpilot run "python3 -c 'print(1+1)'"
```

```python
from sandboxpilot import Sandbox

with Sandbox.create() as sb:
    sb.write("/work/hello.py", "print('hi from gVisor')")
    result = sb.run("python3 /work/hello.py")
    print(result.stdout)          # hi from gVisor
```

```typescript
import { Sandbox } from "@sandboxpilot/sdk";

const sb = await Sandbox.create();
console.log((await sb.run("uname -a")).stdout);
await sb.kill();
```

When you are done for the day: `sandboxpilot down`. Workers are terminated, billing stops.

## How it works

- **Workers** are cloud VMs launched by SkyPilot. One worker hosts many sandboxes.
- **Sandboxes** are gVisor containers on a worker. Small, fast to create, killed on timeout.
- **The control plane** runs on your machine (`127.0.0.1:7070`, started automatically by the SDK).
  It talks to workers over SSH tunnels. State is in SQLite. Nothing is exposed to the internet.

Clouds are picked by SkyPilot. By default it tries AWS, GCP and Azure and takes the cheapest.
Pin one with `sandboxpilot up --cloud aws`.

## CLI

```
sandboxpilot doctor                      check control plane, credentials, SkyPilot, ssh, workers
sandboxpilot up [--cloud X] [--workers N] start warm workers
sandboxpilot down                        stop all workers
sandboxpilot status                      pools, workers, sandboxes
sandboxpilot run "<cmd>"                 run in a fresh sandbox
sandboxpilot create [--image X]          create a sandbox, print its id
sandboxpilot exec <id> "<cmd>"           run in an existing sandbox
sandboxpilot cp <src> <id>:<dst>         copy files in or out
sandboxpilot kill <id>                   kill a sandbox
sandboxpilot bench                       measure startup latency
```

Every command can print JSON: put `--json` right after `sandboxpilot`, e.g.
`sandboxpilot --json sandbox get <id>`. Full list: `sandboxpilot --help`.

## SDK

Python (sync and async) and TypeScript (`npm install @sandboxpilot/sdk`) share the same API:
`create`, `run`, `run_background`, `stream`, `write`, `read`, `upload`, `download`,
`get_url` (expose a port), `set_timeout`, `kill`. See [docs/sdk.md](docs/sdk.md).

## Docs

- [Cloud setup (AWS, GCP, Azure)](docs/clouds.md)
- [Pools, sizing and scaling](docs/pools.md)
- [SDK reference](docs/sdk.md)
- [Architecture](docs/architecture.md)
- [Security model](docs/security.md)
- [Troubleshooting](docs/troubleshooting.md)

## Development

```bash
uv sync --extra dev
uv run pytest                 # fake provider + fake runtime, no cloud needed
uv run ruff format --check src tests && uv run ruff check src tests && uv run mypy src
cd sdk/typescript && npm ci && npm run lint && npm test
```

Real gVisor and cloud tests are opt-in: `pytest -m gvisor` on a Linux box with `runsc`,
`pytest -m cloud_aws` with credentials.

## License

Apache 2.0
