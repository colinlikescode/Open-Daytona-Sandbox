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
  CommandLogs,
  CommandResult,
  CreateSandboxOptions,
  DoctorReport,
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
      code: "command_error",
      message: `command exited with ${result.exit_code}: ${result.stderr.trim().slice(0, 500)}`,
      details: { exit_code: result.exit_code },
    });
    this.name = "CommandError";
    this.result = result;
  }
}

export class CommandTimeoutError extends SandboxPilotError {
  readonly result: CommandResult;
  constructor(result: CommandResult, timeout?: number) {
    super(408, {
      code: "command_timeout",
      message: timeout !== undefined ? `command timed out after ${timeout}s` : "command timed out",
    });
    this.name = "CommandTimeoutError";
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

type Query = Record<string, string | number | boolean | undefined>;

function envVar(name: string): string | undefined {
  const p = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
  return p?.env?.[name];
}

function queryString(query?: Query): string {
  if (!query) return "";
  const pairs = Object.entries(query)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  return pairs.length ? `?${pairs.join("&")}` : "";
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

  async request(method: string, path: string, init: RequestInit & { query?: Query } = {}): Promise<Response> {
    const { query, ...rest } = init;
    const headers = new Headers(rest.headers);
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    let resp: Response;
    try {
      resp = await this.fetchImpl(`${this.url}/v1${path}${queryString(query)}`, { ...rest, method, headers });
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
        const body = (await resp.json()) as { error?: ApiErrorPayload; detail?: unknown };
        if (body.error) payload = body.error;
        else if (body.detail !== undefined) payload = { code: "http_error", message: String(body.detail) };
      } catch {
        /* non-JSON error body */
      }
      throw new SandboxPilotError(resp.status, payload);
    }
    return resp;
  }

  async json<T>(method: string, path: string, body?: unknown, query?: Query, headers?: Record<string, string>): Promise<T> {
    const resp = await this.request(method, path, {
      query,
      headers: { ...(body !== undefined ? { "Content-Type": "application/json" } : {}), ...headers },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    const text = await resp.text();
    return (text ? JSON.parse(text) : null) as T;
  }

  // -- system ------------------------------------------------------------------

  health(): Promise<{ status: string; version: string; api_version: string; provider: string }> {
    return this.json("GET", "/health");
  }
  status(): Promise<Record<string, unknown>> {
    return this.json("GET", "/status");
  }
  doctor(): Promise<DoctorReport> {
    return this.json("GET", "/doctor");
  }

  // -- pools / workers ---------------------------------------------------------

  listPools(): Promise<WorkerPool[]> {
    return this.json("GET", "/pools");
  }
  getPool(name: string): Promise<WorkerPool> {
    return this.json("GET", `/pools/${encodeURIComponent(name)}`);
  }
  poolUp(name = "default", workers?: number): Promise<Operation[]> {
    return this.json("POST", `/pools/${encodeURIComponent(name)}/up`, workers !== undefined ? { workers } : {});
  }
  poolDown(name = "default", force = false): Promise<Record<string, number>> {
    return this.json("POST", `/pools/${encodeURIComponent(name)}/down`, undefined, { force });
  }
  listWorkers(pool?: string): Promise<WorkerView[]> {
    return this.json("GET", "/workers", undefined, { pool });
  }
  getOperation(id: string, waitSeconds = 0): Promise<Operation> {
    return this.json("GET", `/operations/${encodeURIComponent(id)}`, undefined, { wait: waitSeconds });
  }
  /** Wait for an operation to finish; throws if it failed, was cancelled, or did not finish in time. */
  async waitOperation(id: string, timeoutSeconds = 900): Promise<Operation> {
    const op = await this.getOperation(id, timeoutSeconds);
    if (op.status === "FAILED" || op.status === "CANCELLED") {
      throw new SandboxPilotError(500, { code: "operation_failed", message: op.error ?? `operation ${op.status.toLowerCase()}` });
    }
    if (op.status !== "SUCCEEDED") {
      throw new SandboxPilotError(504, { code: "operation_timeout", message: `operation ${id} did not finish within ${timeoutSeconds}s` });
    }
    return op;
  }

  // -- sandboxes ---------------------------------------------------------------

  listSandboxes(opts: { pool?: string; all?: boolean } = {}): Promise<SandboxInfo[]> {
    return this.json("GET", "/sandboxes", undefined, { pool: opts.pool, all: opts.all ?? false });
  }
  getSandboxInfo(id: string): Promise<SandboxInfo> {
    return this.json("GET", `/sandboxes/${encodeURIComponent(id)}`);
  }
  createSandbox(opts: CreateSandboxOptions = {}): Promise<SandboxInfo> {
    const { createTimeout, idempotencyKey, ...rest } = opts;
    const body: Record<string, unknown> = { ...rest };
    if (createTimeout !== undefined) body.create_timeout = createTimeout;
    return this.json("POST", "/sandboxes", body, undefined, idempotencyKey ? { "Idempotency-Key": idempotencyKey } : undefined);
  }
  killSandbox(id: string): Promise<SandboxInfo> {
    return this.json("DELETE", `/sandboxes/${encodeURIComponent(id)}`);
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

  private get base(): string {
    return `/sandboxes/${encodeURIComponent(this.id)}`;
  }

  async refresh(): Promise<SandboxInfo> {
    this.info = await this.client.getSandboxInfo(this.id);
    return this.info;
  }

  async kill(): Promise<void> {
    this.info = await this.client.killSandbox(this.id);
  }

  /** Set a new lifetime from now (seconds or a duration string such as "30m"). */
  async setTimeout(timeout: string | number): Promise<SandboxInfo> {
    this.info = await this.client.json("POST", `${this.base}/timeout`, { timeout });
    return this.info;
  }

  /** Run a command to completion. */
  async run(command: string | string[], opts: RunOptions = {}): Promise<CommandResult> {
    const result = await this.client.json<CommandResult>("POST", `${this.base}/exec`, commandBody(command, opts));
    if (result.status === "TIMED_OUT") throw new CommandTimeoutError(result, opts.timeout);
    if (opts.check && result.exit_code !== 0) throw new CommandError(result);
    return result;
  }

  /** Start a command and return a handle without waiting. */
  async runBackground(command: string | string[], opts: RunOptions = {}): Promise<Command> {
    const info = await this.client.json<CommandInfo>("POST", `${this.base}/exec/start`, commandBody(command, opts, true));
    return new Command(this, info);
  }

  /** Start a command and stream its stdout/stderr/exit events. */
  async *stream(command: string | string[], opts: RunOptions = {}): AsyncGenerator<CommandEvent> {
    const cmd = await this.runBackground(command, opts);
    yield* cmd.events();
  }

  async write(path: string, content: string | Uint8Array, mode?: number): Promise<void> {
    const data = typeof content === "string" ? new TextEncoder().encode(content) : content;
    // A Blob body has a known size, so fetch sets Content-Length itself and the
    // control plane can stream the upload without buffering it.
    await this.client.request("PUT", `${this.base}/files`, {
      query: { path, mode },
      body: new Blob([data as BlobPart]),
      headers: { "Content-Type": "application/octet-stream" },
    });
  }

  async read(path: string): Promise<Uint8Array> {
    const resp = await this.client.request("GET", `${this.base}/files`, { query: { path } });
    return new Uint8Array(await resp.arrayBuffer());
  }

  async readText(path: string): Promise<string> {
    return new TextDecoder().decode(await this.read(path));
  }

  /** Signed URL that proxies to `port` inside the sandbox (HTTP and WebSocket). */
  async getUrl(port: number, expiresIn?: number): Promise<string> {
    const data = await this.client.json<{ url: string }>("POST", `${this.base}/url`, { port, expires_in: expiresIn ?? null });
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
    return `/sandboxes/${encodeURIComponent(this.sandbox.id)}/exec/${encodeURIComponent(this.id)}`;
  }

  async refresh(): Promise<CommandInfo> {
    this.info = await this.sandbox.client.json("GET", this.base);
    return this.info;
  }

  wait(): Promise<CommandResult> {
    return this.sandbox.client.json("GET", `${this.base}/result`, undefined, { wait: true });
  }

  logs(): Promise<CommandLogs> {
    return this.sandbox.client.json("GET", `${this.base}/logs`);
  }

  async kill(): Promise<CommandInfo> {
    this.info = await this.sandbox.client.json("DELETE", this.base);
    return this.info;
  }

  async *events(fromSeq = 0): AsyncGenerator<CommandEvent> {
    const resp = await this.sandbox.client.request("GET", `${this.base}/stream`, { query: { from_seq: fromSeq } });
    if (!resp.body) return;
    yield* parseSse(resp.body);
  }
}

/** Parse a server-sent-events byte stream into CommandEvents (handles \n and \r\n framing). */
export async function* parseSse(body: ReadableStream<Uint8Array>): AsyncGenerator<CommandEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const emit = function* (block: string): Generator<CommandEvent> {
    const data = block
      .split(/\r?\n/)
      .filter((l) => l.startsWith("data:"))
      .map((l) => l.slice(5).trim())
      .join("\n");
    if (data) yield JSON.parse(data) as CommandEvent;
  };
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let m: RegExpMatchArray | null;
      while ((m = buffer.match(/\r?\n\r?\n/)) && m.index !== undefined) {
        const block = buffer.slice(0, m.index);
        buffer = buffer.slice(m.index + m[0].length);
        yield* emit(block);
      }
    }
    buffer += decoder.decode();
    if (buffer.trim()) yield* emit(buffer);
  } finally {
    reader.releaseLock();
  }
}
