# @sandboxpilot/sdk

TypeScript client for [SandboxPilot](https://github.com/sandboxpilot/sandboxpilot): fast gVisor
sandboxes on VMs in your own AWS, GCP or Azure account.

```bash
npm install @sandboxpilot/sdk
sandboxpilot daemon start        # the SDK talks to the local control plane (Python package)
```

```typescript
import { Sandbox } from "@sandboxpilot/sdk";

const sb = await Sandbox.create({ image: "python:3.12-slim", timeout: "30m" });
try {
  await sb.write("/workspace/hello.py", "print('hi from gVisor')");
  const r = await sb.run("python3 /workspace/hello.py", { check: true });
  console.log(r.stdout);

  for await (const ev of sb.stream("for i in 1 2 3; do echo $i; sleep 1; done")) {
    if (ev.type === "stdout") process.stdout.write(ev.text);
  }

  const url = await sb.getUrl(8080); // signed URL proxied to a port inside the sandbox
} finally {
  await sb.kill();
}
```

Configuration: `SANDBOXPILOT_API_URL` (default `http://127.0.0.1:7070`) and
`SANDBOXPILOT_API_TOKEN` (required when the control plane is not on loopback), or pass
`new SandboxPilot({ url, token })` to `Sandbox.create`/`Sandbox.connect`.

Unlike the Python SDK this client cannot start the control plane for you: run
`sandboxpilot daemon start` first. Requires Node 18+ (global `fetch`).

Full API and wire types: see `docs/sdk.md` in the main repository.
