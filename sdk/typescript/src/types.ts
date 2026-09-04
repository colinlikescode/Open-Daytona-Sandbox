// Wire types. These mirror the Pydantic models in the Python package; keep them
// in sync when the REST API changes (see docs/api.md).

export type SandboxState =
  | "PENDING"
  | "CREATING"
  | "RUNNING"
  | "STOPPING"
  | "STOPPED"
  | "FAILED"
  | "LOST"
  | "EXPIRED";

export type NetworkPolicy = "internet" | "none";

export type CommandStatus = "RUNNING" | "EXITED" | "TIMED_OUT" | "KILLED" | "FAILED";

export interface SandboxInfo {
  id: string;
  short_id: string;
  pool: string;
  worker_id: string | null;
  state: SandboxState;
  image: string;
  cpus: number;
  memory_bytes: number;
  pids_limit: number;
  network: NetworkPolicy;
  workdir: string;
  labels: Record<string, string>;
  metadata: Record<string, string>;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
  expires_at: string | null;
  error: string | null;
  metrics: Record<string, number>;
  env_keys: string[];
}

export interface CreateSandboxOptions {
  image?: string;
  pool?: string;
  template?: string;
  cpus?: number;
  memory?: string | number;
  timeout?: string | number;
  env?: Record<string, string>;
  workdir?: string;
  network?: NetworkPolicy;
  user?: string;
  labels?: Record<string, string>;
  metadata?: Record<string, string>;
  createTimeout?: number;
  idempotencyKey?: string;
}

export interface RunOptions {
  env?: Record<string, string>;
  cwd?: string;
  timeout?: number;
  user?: string;
  /** Throw CommandError on a non-zero exit code. */
  check?: boolean;
}

export interface CommandResult {
  command_id: string;
  exit_code: number;
  stdout: string;
  stderr: string;
  started_at: string;
  finished_at: string;
  status: CommandStatus;
  output_truncated: boolean;
  error: string | null;
}

export interface CommandInfo {
  command_id: string;
  sandbox_id: string;
  status: CommandStatus;
  exit_code: number | null;
  started_at: string;
  finished_at: string | null;
  display: string;
  error: string | null;
}

export interface CommandEvent {
  type: "stdout" | "stderr" | "exit" | "error";
  text: string;
  exit_code: number | null;
  status: CommandStatus | null;
  seq: number;
}

export interface WorkerCapacity {
  cpu_millis_total: number;
  cpu_millis_allocatable: number;
  memory_bytes_total: number;
  memory_bytes_allocatable: number;
  cpu_millis_allocated: number;
  memory_bytes_allocated: number;
  sandboxes_running: number;
  sandboxes_warm: number;
  sandboxes_pending: number;
}

export interface WorkerView {
  id: string;
  short_id: string;
  pool_id: string;
  pool_name: string;
  state: string;
  cloud: string | null;
  region: string | null;
  instance_type: string | null;
  use_spot: boolean;
  hourly_cost: number | null;
  capacity: WorkerCapacity;
  draining: boolean;
  provision_seconds: number | null;
}

export interface WorkerPool {
  id: string;
  name: string;
  min_workers: number;
  max_workers: number;
  worker_cpus: number;
  worker_memory_bytes: number;
  use_spot: boolean;
  warm_slots: number;
  default_image: string;
  runtime: string;
  [key: string]: unknown;
}

export interface Operation {
  id: string;
  type: string;
  status: "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
  worker_id: string | null;
  error: string | null;
  result: Record<string, unknown>;
}

export interface ApiErrorPayload {
  code: string;
  message: string;
  hint?: string | null;
  details?: Record<string, unknown>;
}
