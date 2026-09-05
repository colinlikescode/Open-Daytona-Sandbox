# SDK reference

Python (sync and async) and TypeScript expose the same surface. Every method is one
call to the control plane REST API (`/v1`); nothing talks to workers directly.

## Connecting

| | Python | TypeScript |
|---|---|---|
| Default URL | `SANDBOXPILOT_API_URL`, else `api.url` from `config.yaml`, else `http://127.0.0.1:7070` | `SANDBOXPILOT_API_URL`, else `http://127.0.0.1:7070` |
| Token | `SANDBOXPILOT_API_TOKEN` or `api.token` (only needed off-loopback) | `SANDBOXPILOT_API_TOKEN` |
| Autostart | Yes: if nothing answers on a loopback URL, `sandboxpilot daemon run` is spawned and awaited (`autostart=False` to disable) | No: run `sandboxpilot daemon start` first |

```python
from sandboxpilot import Sandbox, SandboxPilot, AsyncSandbox, AsyncSandboxPilot

client = SandboxPilot()                       # or SandboxPilot(url, token, autostart=False)
sb = Sandbox.create(client=client)            # shares one client across sandboxes
```

`AsyncSandboxPilot()` does no I/O in its constructor; the control plane is located
(and started) on the first request or via `await client.connect()`. The event loop is
never blocked: config parsing runs in a thread, health checks use an async client.

## Sandbox lifecycle

```python
sb = Sandbox.create(
    image="python:3.12-slim",   # default: pool's sandbox.image
    pool=None, template=None,
    cpus=1.0, memory="2GB",     # sizes accept "512MB", "1Gi", bytes
    timeout="1h",               # auto-kill after; capped by pool sandbox.max_timeout
    env={"KEY": "value"},       # values are never returned by the API (only env_keys)
    workdir="/workspace", network="internet",  # or "none"
    labels={}, metadata={},
    create_timeout=900,         # seconds to wait for capacity (worker provisioning included)
    idempotency_key=None,       # repeats with the same key return the same sandbox
)
sb.id, sb.short_id, sb.info    # SandboxInfo: state, image, worker_id, expires_at, metrics, ...
sb.refresh()                   # re-read SandboxInfo
sb.set_timeout("30m")          # new lifetime from now
sb.kill()                      # idempotent; terminal states: STOPPED, FAILED, LOST, EXPIRED
Sandbox.connect(sandbox_id)    # attach to an existing sandbox
```

`with Sandbox.create() as sb:` kills the sandbox on exit. Killing a sandbox that is
still waiting for capacity cancels the create.

## Commands

```python
r = sb.run("python3 -c 'print(1)'", env=None, cwd=None, timeout=None, user=None, check=False)
r.exit_code, r.stdout, r.stderr, r.status, r.output_truncated, r.duration_seconds
```

- A string runs through `/bin/sh -lc`; a list is executed directly (no shell).
- `timeout` (seconds) kills the process tree and raises `CommandTimeoutError`.
- `check=True` raises `CommandError` on a non-zero exit.
- Output is capped (default 4 MiB per stream, head + tail kept; `output_truncated`).

```python
cmd = sb.run_background("make -j")      # Command / AsyncCommand
cmd.info, cmd.refresh(), cmd.logs(), cmd.kill(), cmd.wait()   # wait() -> CommandResult
for ev in sb.stream("pytest -q"):        # CommandEvent: type stdout|stderr|exit|error, text, exit_code, seq
    ...
for ev in cmd.events(from_seq=0): ...    # replay + follow an existing command
```

## Files

```python
sb.write("/work/a.txt", "text or bytes", mode=0o644)   # parent directories are created
sb.read("/work/a.txt") -> bytes;  sb.read_text(path)
sb.upload(local_path, "/remote/path")        # file, or directory (sent as a tar)
sb.download("/remote/file", "./local")       # file into path/dir; directories are extracted under ./local
```

Uploads and downloads are capped by `limits.max_upload_size` / `max_download_size`
(512 MiB by default). Paths must be absolute.

## Network

```python
url = sb.get_url(8080, expires_in=3600)
# http://127.0.0.1:7070/v1/proxy/<sandbox_id>/8080/<signed-token>/
```

The URL proxies HTTP and WebSocket traffic to that port inside the sandbox. The token
is an HMAC over sandbox id, port and expiry; it carries no API token, so it can be
handed to a browser. Set `api.external_url` when clients reach the control plane
through another address.

## Administration (client object)

```python
client.status(); client.doctor(); client.metrics(); client.cleanup(terminate_workers=False)
client.list_pools(); client.get_pool(name); client.create_pool(PoolCreateRequest(...))
client.update_pool(name, PoolUpdateRequest(...)); client.delete_pool(name, force=False)
client.pool_up(name, workers=None) -> [Operation]; client.pool_down(name, force=False)
client.list_workers(pool=None); client.get_worker(ref); client.drain_worker(ref); client.remove_worker(ref, force=False)
client.get_operation(op_id, wait=0); client.wait_operation(op_id, timeout=900)
client.preload_image("node:22-slim", pool=None); client.list_images(worker=None)
client.list_templates(); client.add_template(Template(...)); client.remove_template(name)
client.list_sandboxes(pool=None, all=False); client.get_sandbox_info(ref); client.kill_sandbox(ref)
```

## Errors

All errors derive from `SandboxPilotError` and carry `.code`, `.message`, `.hint`,
`.details`. The REST API returns the same shape:
`{"error": {"code", "message", "hint", "details"}}`.

| code | class | when |
|---|---|---|
| `validation_error` | `ValidationError` | bad request (also raised locally for bad sizes/durations) |
| `no_capacity` | `NoCapacityError` | the request can never fit a worker of this pool |
| `create_timeout` | `CreateTimeoutError` | no capacity within `create_timeout` |
| `sandbox_not_found` / `sandbox_not_running` / `sandbox_lost` | ... | lifecycle |
| `command_timeout` / `command_error` | `CommandTimeoutError` / `CommandError` | commands |
| `worker_unavailable` | `WorkerUnavailableError` | tunnel or worker down |
| `worker_provision_error` | `WorkerProvisionError` | SkyPilot could not deliver a VM |
| `authentication_error` | `AuthenticationError` | missing/invalid API token or proxy token |

## TypeScript

```typescript
import { Sandbox, SandboxPilot } from "@sandboxpilot/sdk";

const client = new SandboxPilot({ url, token });        // both optional, env-driven
const sb = await Sandbox.create({ image: "node:22-slim", timeout: "30m" }, client);
await sb.run("npm test", { check: true });
const cmd = await sb.runBackground("npm start");
for await (const ev of cmd.events()) { ... }
await sb.write("/workspace/a.txt", "abc"); await sb.readText("/workspace/a.txt");
await sb.getUrl(3000); await sb.setTimeout("1h"); await sb.kill();
```

Option names are the wire names (`read_only_root`, `pids_limit`, ...) except
`createTimeout` and `idempotencyKey`. Wire types are exported from the package and
mirror the Pydantic models in `src/sandboxpilot/schemas`.
