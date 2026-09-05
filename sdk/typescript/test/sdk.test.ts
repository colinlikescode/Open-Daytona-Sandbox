// Unit tests use a fake fetch. The e2e block needs a running control plane:
//   SANDBOXPILOT_E2E=1 SANDBOXPILOT_API_URL=http://127.0.0.1:7070 npm test
import { describe, expect, it } from "vitest";

import {
  Command,
  CommandError,
  CommandTimeoutError,
  Sandbox,
  SandboxPilot,
  SandboxPilotError,
  TERMINAL_SANDBOX_STATES,
  parseSse,
} from "../src/index.js";
import type { CommandInfo, CommandResult, Operation, SandboxInfo } from "../src/index.js";

const info: SandboxInfo = {
  id: "sbx_1",
  short_id: "1",
  pool: "default",
  worker_id: "wrk_1",
  state: "RUNNING",
  image: "python:3.12-slim",
  cpus: 1,
  memory_bytes: 1024,
  pids_limit: 512,
  network: "internet",
  workdir: "/workspace",
  labels: {},
  metadata: {},
  created_at: "2026-01-01T00:00:00Z",
  started_at: null,
  ended_at: null,
  expires_at: null,
  error: null,
  metrics: { warm_slot: 1 },
  env_keys: [],
  worker_state: "HEALTHY",
};

const cmdInfo: CommandInfo = {
  command_id: "cmd_1",
  sandbox_id: "sbx_1",
  status: "RUNNING",
  exit_code: null,
  started_at: "",
  finished_at: null,
  display: "",
  error: null,
};

function fakeFetch(handler: (url: string, init: RequestInit) => Response | Promise<Response>): typeof fetch {
  return (async (input: string | URL | Request, init?: RequestInit) => handler(String(input), init ?? {})) as typeof fetch;
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

function sseStream(text: string): ReadableStream<Uint8Array> {
  return new Response(text).body as ReadableStream<Uint8Array>;
}

describe("SandboxPilot client", () => {
  it("sends bearer token and maps API errors", async () => {
    const seen: string[] = [];
    const client = new SandboxPilot({
      url: "http://cp",
      token: "tok",
      fetch: fakeFetch((url, init) => {
        seen.push(`${init.method} ${url} ${new Headers(init.headers).get("authorization")}`);
        return json({ error: { code: "sandbox_not_found", message: "nope", hint: "check id" } }, 404);
      }),
    });
    await expect(client.getSandboxInfo("sbx_x")).rejects.toMatchObject({ code: "sandbox_not_found", status: 404, hint: "check id" });
    expect(seen[0]).toBe("GET http://cp/v1/sandboxes/sbx_x Bearer tok");
  });

  it("creates a sandbox with an idempotency key and runs commands", async () => {
    const calls: Array<{ url: string; body: unknown; headers: Headers }> = [];
    const result: CommandResult = {
      command_id: "cmd_1",
      exit_code: 3,
      stdout: "out",
      stderr: "bad",
      started_at: "",
      finished_at: "",
      status: "EXITED",
      output_truncated: false,
      error: null,
    };
    const client = new SandboxPilot({
      url: "http://cp",
      fetch: fakeFetch((url, init) => {
        calls.push({ url, body: init.body ? JSON.parse(String(init.body)) : undefined, headers: new Headers(init.headers) });
        if (url.endsWith("/v1/sandboxes")) return json(info, 201);
        if (url.endsWith("/exec")) return json(result);
        return json(info);
      }),
    });
    const sb = await Sandbox.create(
      { image: "python:3.12-slim", env: { A: "1" }, idempotencyKey: "k1", createTimeout: 30, read_only_root: true },
      client,
    );
    expect(sb.id).toBe("sbx_1");
    expect(calls[0].headers.get("idempotency-key")).toBe("k1");
    expect(calls[0].body).toMatchObject({ image: "python:3.12-slim", env: { A: "1" }, create_timeout: 30, read_only_root: true });
    expect(calls[0].body).not.toHaveProperty("createTimeout");

    const r = await sb.run(["sh", "-c", "exit 3"], { cwd: "/tmp" });
    expect(r.exit_code).toBe(3);
    expect(calls[1].body).toMatchObject({ args: ["sh", "-c", "exit 3"], cwd: "/tmp", background: false });
    await expect(sb.run("exit 3", { check: true })).rejects.toBeInstanceOf(CommandError);
    await expect(sb.run("exit 3", { check: true })).rejects.toMatchObject({ code: "command_error" });
  });

  it("raises CommandTimeoutError when the control plane reports a timeout", async () => {
    const timedOut: CommandResult = {
      command_id: "cmd_1",
      exit_code: 137,
      stdout: "",
      stderr: "",
      started_at: "",
      finished_at: "",
      status: "TIMED_OUT",
      output_truncated: false,
      error: "Command timed out after 1s",
    };
    const client = new SandboxPilot({
      url: "http://cp",
      fetch: fakeFetch((url) => (url.endsWith("/exec") ? json(timedOut) : json(info))),
    });
    const sb = await Sandbox.connect("sbx_1", client);
    await expect(sb.run("sleep 5", { timeout: 1 })).rejects.toBeInstanceOf(CommandTimeoutError);
  });

  it("parses SSE streams, including CRLF framing", async () => {
    const sse =
      'event: stdout\ndata: {"type":"stdout","text":"hi\\n","exit_code":null,"status":null,"seq":1}\n\n' +
      'event: exit\ndata: {"type":"exit","text":"","exit_code":0,"status":"EXITED","seq":2}\n\n';
    const client = new SandboxPilot({
      url: "http://cp",
      fetch: fakeFetch((url) => {
        if (url.endsWith("/exec/start")) return json(cmdInfo, 202);
        if (url.includes("/stream")) return new Response(sse, { status: 200 });
        return json(info);
      }),
    });
    const sb = await Sandbox.connect("sbx_1", client);
    const events = [];
    for await (const ev of sb.stream("echo hi")) events.push(ev);
    expect(events.map((e) => e.type)).toEqual(["stdout", "exit"]);
    expect(events[0].text).toBe("hi\n");
    expect(new Command(sb, cmdInfo).id).toBe("cmd_1");

    const crlf = sse.replace(/\n/g, "\r\n");
    const parsed = [];
    for await (const ev of parseSse(sseStream(crlf))) parsed.push(ev.type);
    expect(parsed).toEqual(["stdout", "exit"]);
  });

  it("fetches background command logs", async () => {
    const client = new SandboxPilot({
      url: "http://cp",
      fetch: fakeFetch((url) => {
        if (url.endsWith("/exec/start")) return json(cmdInfo, 202);
        if (url.endsWith("/logs")) return json({ command_id: "cmd_1", stdout: "partial", stderr: "", truncated: false });
        return json(info);
      }),
    });
    const sb = await Sandbox.connect("sbx_1", client);
    const cmd = await sb.runBackground("long-running");
    expect((await cmd.logs()).stdout).toBe("partial");
  });

  it("writes files with a sized body and no hand-rolled Content-Length", async () => {
    let seen: RequestInit | undefined;
    const client = new SandboxPilot({
      url: "http://cp",
      fetch: fakeFetch((url, init) => {
        if (init.method !== "PUT") return json(info);
        seen = init;
        expect(url).toContain("/files?path=%2Fa&mode=384");
        return json({ path: "/a", bytes: 3 });
      }),
    });
    const sb = await Sandbox.connect("sbx_1", client);
    await sb.write("/a", "abc", 0o600);
    expect(seen?.body).toBeInstanceOf(Blob);
    expect((seen?.body as Blob).size).toBe(3);
    expect(new Headers(seen?.headers).has("content-length")).toBe(false);
  });

  it("waitOperation distinguishes success, failure, cancellation and timeout", async () => {
    const op = (status: Operation["status"], error: string | null = null): Operation => ({
      id: "op_1",
      type: "worker.provision",
      status,
      pool_id: null,
      worker_id: null,
      sandbox_id: null,
      created_at: "",
      started_at: null,
      completed_at: null,
      error,
      result: {},
    });
    let next: Operation = op("SUCCEEDED");
    const client = new SandboxPilot({ url: "http://cp", fetch: fakeFetch(() => json(next)) });
    expect((await client.waitOperation("op_1")).status).toBe("SUCCEEDED");
    next = op("FAILED", "quota");
    await expect(client.waitOperation("op_1")).rejects.toMatchObject({ code: "operation_failed", message: "quota" });
    next = op("CANCELLED");
    await expect(client.waitOperation("op_1")).rejects.toMatchObject({ code: "operation_failed" });
    next = op("RUNNING");
    await expect(client.waitOperation("op_1", 5)).rejects.toMatchObject({ code: "operation_timeout" });
  });

  it("wraps network failures", async () => {
    const client = new SandboxPilot({ url: "http://cp", fetch: fakeFetch(() => { throw new Error("ECONNREFUSED"); }) });
    await expect(client.health()).rejects.toBeInstanceOf(SandboxPilotError);
  });

  it("exposes terminal states", () => {
    expect(TERMINAL_SANDBOX_STATES.has("EXPIRED")).toBe(true);
    expect(TERMINAL_SANDBOX_STATES.has("RUNNING")).toBe(false);
  });
});

describe.skipIf(!process.env.SANDBOXPILOT_E2E)("e2e against a live control plane", () => {
  it("creates, runs, streams, reads files, kills", async () => {
    const sb = await Sandbox.create({ timeout: "5m", env: { NAME: "ts" } });
    try {
      expect(sb.info.state).toBe("RUNNING");
      const r = await sb.run("echo hello $NAME");
      expect(r.stdout.trim()).toBe("hello ts");
      await sb.write("/workspace/a.txt", "abc");
      expect(await sb.readText("/workspace/a.txt")).toBe("abc");
      const types: string[] = [];
      for await (const ev of sb.stream("echo x; exit 2")) types.push(ev.type);
      expect(types.at(-1)).toBe("exit");
      expect(await sb.getUrl(8080)).toContain("/v1/proxy/");
    } finally {
      await sb.kill();
    }
    expect((await sb.refresh()).state).toBe("STOPPED");
  }, 120_000);
});
