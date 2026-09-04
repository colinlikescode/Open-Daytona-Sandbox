// SandboxPilot TypeScript SDK.
//
//   import { Sandbox } from "@sandboxpilot/sdk";
//   const sb = await Sandbox.create({ image: "python:3.12-slim" });
//   const r = await sb.run("python3 -c 'print(1+1)'");
//   await sb.kill();
//
// Talks to the control plane REST API only. Uses the global fetch (Node 18+).
// Unlike the Python SDK it cannot start a local control plane for you; run
// `sandboxpilot daemon start` first, or point it at a remote one.

import type {
  ApiErrorPayload,
  CommandEvent,
  CommandInfo,
  CommandResult,
  CreateSandboxOptions,
  Operation,
  RunOptions,
  SandboxInfo,
  WorkerPool,
  WorkerView,
} from "./types.js";

export * from "./types.js";

export class SandboxPilotError extends Error {
  readonly code: string;
  readonly status: number;
  readonly hint?: string;
  readonly details?: Record<string, unknown>;

  constructor(status: number, payload: ApiErrorPayload) {
    super(payload.message);
    this.name = "SandboxPilotError";
    this.code = payload.code;
    this.status = status;
    this.hint = payload.hint ?? undefined;
    this.details = payload.details;
  }
}

export class CommandError extends SandboxPilotError {
  readonly result: CommandResult;
  constructor(result: CommandResult) {
    super(0, {
      code: "command_failed",
      message: `command exited with ${result.exit_code}: ${result.stderr.trim().slice(0, 500)}`,
    });
    this.name = "CommandError";
    this.result = result;
  }
}

export interface ClientOptions {
  /** Control plane URL. Default: SANDBOXPILOT_API_URL or http://127.0.0.1:7070 */
  url?: string;
  /** Bearer token. Default: SANDBOXPILOT_API_TOKEN */
  token?: string;
  fetch?: typeof fetch;
}

function envVar(name: string): string | undefined {
  const p = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
  return p?.env?.[name];
}

export class SandboxPilot {
  readonly url: string;
  private readonly token?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: ClientOptions = {}) {
    this.url = (opts.url ?? envVar("SANDBOXPILOT_API_URL") ?? "http://127.0.0.1:7070").replace(/\/$/, "");
    this.token = opts.token ?? envVar("SANDBOXPILOT_API_TOKEN");
    this.fetchImpl = opts.fetch ?? fetch;
  }

  // -- raw ---------------------------------------------------------------------

  async request(method: string, path: string, init: RequestInit & { query?: Record<string, string | number | boolean | undefined> } = {}): Promise<Response> {
    const { query, ...rest } = init;
    const qs = query
      ? "?" +
        Object.entries(query)
          .filter(([, v]) => v !== undefined)
          .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
          .join("&")
      : "";
    const headers = new Headers(rest.headers);
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    let resp: Response;
    try {
      resp = await this.fetchImpl(`${this.url}/v1${path}${qs}`, { ...rest, method, headers });
    } catch (err) {
      throw new SandboxPilotError(0, {
        code: "unreachable",
        message: `Could not reach the SandboxPilot control plane at ${this.url}: ${String(err)}`,
        hint: "Is it running? Try: sandboxpilot daemon start",
      });
    }
    if (!resp.ok) {
      let payload: ApiErrorPayload = { code: "http_error", message: `HTTP ${resp.status}` };
      try {
        const body = (await resp.json()) as { error?: ApiErrorPayload };
        if (body.error) payload = body.error;
      } catch {
        /* non-JSON error body */
      }
      throw new SandboxPilotError(resp.status, payload);
    }
    return resp;
  }

  async json<T>(method: string, path: string, body?: unknown, query?: Record<string, string | number | boolean | undefined>, headers?: Record<string, string>): Promise<T> {
    const resp = await this.request(method, path, {
      query,
      headers: { ...(body !== undefined ? { "Content-Type": "application/json" } : {}), ...headers },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    const text = await resp.text();
    return (text ? JSON.parse(text) : null) as T;
  }

  // -- system ------------------------------------------------------------------

  health(): Promise<{ status: string; version: string }> {
    return this.json("GET", "/health");
  }
  status(): Promise<Record<string, unknown>> {
    return this.json("GET", "/status");
  }

  // -- pools / workers ---------------------------------------------------------

  listPools(): Promise<WorkerPool[]> {
    return this.json("GET", "/pools");
  }
  getPool(name: string): Promise<WorkerPool> {
    return this.json("GET", `/pools/${name}`);
  }
  poolUp(name = "default", workers?: number): Promise<Operation[]> {
    return this.json("POST", `/pools/${name}/up`, workers !== undefined ? { workers } : {});
  }
  poolDown(name = "default", force = false): Promise<Record<string, number>> {
    return this.json("POST", `/pools/${name}/down`, undefined, { force });
  }
  listWorkers(pool?: string): Promise<WorkerView[]> {
    return this.json("GET", "/workers", undefined, { pool });
  }
  async waitOperation(id: string, timeoutSeconds = 900): Promise<Operation> {
    const op = await this.json<Operation>("GET", `/operations/${id}`, undefined, { wait: timeoutSeconds });
    if (op.status === "FAILED") throw new SandboxPilotError(500, { code: "operation_failed", message: op.error ?? "operation failed" });
    return op;
  }

  // -- sandboxes ---------------------------------------------------------------

  listSandboxes(opts: { pool?: string; all?: boolean } = {}): Promise<SandboxInfo[]> {
    return this.json("GET", "/sandboxes", undefined, { pool: opts.pool, all: opts.all ?? false });
  }
  getSandboxInfo(id: string): Promise<SandboxInfo> {
    return this.json("GET", `/sandboxes/${id}`);
  }
  createSandbox(opts: CreateSandboxOptions = {}): Promise<SandboxInfo> {
    const { createTimeout, idempotencyKey, ...rest } = opts;
    const body: Record<string, unknown> = { ...rest };
    if (createTimeout !== undefined) body.create_timeout = createTimeout;
    return this.json("POST", "/sandboxes", body, undefined, idempotencyKey ? { "Idempotency-Key": idempotencyKey } : undefined);
  }
  killSandbox(id: string): Promise<SandboxInfo> {
    return this.json("DELETE", `/sandboxes/${id}`);
  }
}

function commandBody(command: string | string[], opts: RunOptions, background = false): Record<string, unknown> {
  const body: Record<string, unknown> = { env: opts.env ?? {}, background };
  if (typeof command === "string") body.command = command;
  else body.args = command;
  if (opts.cwd) body.cwd = opts.cwd;
  if (opts.timeout) body.timeout = opts.timeout;
  if (opts.user) body.user = opts.user;
  return body;
}

export class Sandbox {
  info: SandboxInfo;
  readonly client: SandboxPilot;

  private constructor(client: SandboxPilot, info: SandboxInfo) {
    this.client = client;
    this.info = info;
  }

  static async create(opts: CreateSandboxOptions = {}, client = new SandboxPilot()): Promise<Sandbox> {
    return new Sandbox(client, await client.createSandbox(opts));
  }

  static async connect(id: string, client = new SandboxPilot()): Promise<Sandbox> {
    return new Sandbox(client, await client.getSandboxInfo(id));
  }

  get id(): string {
    return this.info.id;
  }

  async refresh(): Promise<SandboxInfo> {
    this.info = await this.client.getSandboxInfo(this.id);
    return this.info;
  }

  async kill(): Promise<void> {
    this.info = await this.client.killSandbox(this.id);
  }

  async setTimeout(timeout: string | number): Promise<SandboxInfo> {
    this.info = await this.client.json("POST", `/sandboxes/${this.id}/timeout`, { timeout });
    return this.info;
  }

  /** Run a command to completion. */
  async run(command: string | string[], opts: RunOptions = {}): Promise<CommandResult> {
    const result = await this.client.json<CommandResult>("POST", `/sandboxes/${this.id}/exec`, commandBody(command, opts));
    if (result.status === "TIMED_OUT") {
      throw new SandboxPilotError(408, { code: "command_timeout", message: `command timed out after ${opts.timeout}s` });
    }
    if (opts.check && result.exit_code !== 0) throw new CommandError(result);
    return result;
  }

  /** Start a command and return a handle without waiting. */
  async runBackground(command: string | string[], opts: RunOptions = {}): Promise<Command> {
    const info = await this.client.json<CommandInfo>("POST", `/sandboxes/${this.id}/exec/start`, commandBody(command, opts, true));
    return new Command(this, info);
  }

  /** Start a command and stream its stdout/stderr/exit events. */
  async *stream(command: string | string[], opts: RunOptions = {}): AsyncGenerator<CommandEvent> {
    const cmd = await this.runBackground(command, opts);
    yield* cmd.events();
  }

  async write(path: string, content: string | Uint8Array, mode?: number): Promise<void> {
    const data = typeof content === "string" ? new TextEncoder().encode(content) : content;
    await this.client.request("PUT", `/sandboxes/${this.id}/files`, {
      query: { path, mode },
      body: new Blob([data as BlobPart]),
      headers: { "Content-Type": "application/octet-stream", "Content-Length": String(data.byteLength) },
    });
  }

  async read(path: string): Promise<Uint8Array> {
    const resp = await this.client.request("GET", `/sandboxes/${this.id}/files`, { query: { path } });
    return new Uint8Array(await resp.arrayBuffer());
  }

  async readText(path: string): Promise<string> {
    return new TextDecoder().decode(await this.read(path));
  }

  async getUrl(port: number, expiresIn?: number): Promise<string> {
    const data = await this.client.json<{ url: string }>("POST", `/sandboxes/${this.id}/url`, { port, expires_in: expiresIn ?? null });
    return data.url;
  }
}

export class Command {
  info: CommandInfo;
  readonly sandbox: Sandbox;

  constructor(sandbox: Sandbox, info: CommandInfo) {
    this.sandbox = sandbox;
    this.info = info;
  }

  get id(): string {
    return this.info.command_id;
  }

  private get base(): string {
    return `/sandboxes/${this.sandbox.id}/exec/${this.id}`;
  }

  async refresh(): Promise<CommandInfo> {
    this.info = await this.sandbox.client.json("GET", this.base);
    return this.info;
  }

  wait(): Promise<CommandResult> {
    return this.sandbox.client.json("GET", `${this.base}/result`, undefined, { wait: true });
  }

  async kill(): Promise<CommandInfo> {
    this.info = await this.sandbox.client.json("DELETE", this.base);
    return this.info;
  }

  async *events(fromSeq = 0): AsyncGenerator<CommandEvent> {
    const resp = await this.sandbox.client.request("GET", `${this.base}/stream`, { query: { from_seq: fromSeq } });
    if (!resp.body) return;
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n\n")) >= 0) {
        const block = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        const data = block
          .split("\n")
          .filter((l) => l.startsWith("data:"))
          .map((l) => l.slice(5).trim())
          .join("\n");
        if (data) yield JSON.parse(data) as CommandEvent;
      }
    }
  }
}
