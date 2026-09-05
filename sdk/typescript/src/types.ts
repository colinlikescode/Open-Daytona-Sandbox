// Wire types. These mirror the Pydantic models in the Python package
// (src/sandboxpilot/schemas); keep them in sync when the REST API changes.

export type SandboxState =
  | "PENDING"
  | "WAITING_FOR_CAPACITY"
  | "PROVISIONING_WORKER"
  | "CREATING"
  | "RUNNING"
  | "STOPPING"
  | "STOPPED"
  | "FAILED"
  | "LOST"
  | "EXPIRED";

export type WorkerState =
  | "PROVISIONING"
  | "BOOTSTRAPPING"
  | "CONNECTING"
  | "HEALTHY"
  | "DRAINING"
  | "UNHEALTHY"
  | "TERMINATING"
  | "TERMINATED"
  | "LOST";

export type OperationStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";

export type CloudProvider = "aws" | "gcp" | "azure";

export type NetworkPolicy = "internet" | "none";

export type ImagePullPolicy = "if-not-present" | "always" | "never";

export type CommandStatus = "STARTING" | "RUNNING" | "EXITED" | "FAILED" | "KILLED" | "TIMED_OUT";

/** Terminal sandbox states: the sandbox will never run again. */
export const TERMINAL_SANDBOX_STATES: ReadonlySet<SandboxState> = new Set<SandboxState>([
  "STOPPED",
  "FAILED",
  "LOST",
  "EXPIRED",
]);

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
  /** Names of environment variables set on the sandbox; values are never returned. */
  env_keys: string[];
  worker_state: WorkerState | null;
}

/** Body of POST /v1/sandboxes (snake_case fields go on the wire as-is). */
export interface CreateSandboxOptions {
  image?: string;
  pool?: string;
  template?: string;
  cpus?: number;
  /** Bytes or a size string such as "2GB". */
  memory?: string | number;
  pids_limit?: number;
  /** Seconds or a duration string such as "30m". */
  timeout?: string | number;
  env?: Record<string, string>;
  workdir?: string;
  network?: NetworkPolicy;
  user?: string;
  labels?: Record<string, string>;
  metadata?: Record<string, string>;
  read_only_root?: boolean;
  tmpfs?: string | number;
  keepalive_command?: string[];
  image_pull_policy?: ImagePullPolicy;
  /** Return as soon as the sandbox is queued instead of waiting for RUNNING. */
  wait?: boolean;
  /** Seconds to wait for capacity (sent as create_timeout). */
  createTimeout?: number;
  /** Sent as the Idempotency-Key header: repeats return the same sandbox. */
  idempotencyKey?: string;
}

export interface RunOptions {
  env?: Record<string, string>;
  cwd?: string;
  /** Seconds before the command is killed. */
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

export interface CommandLogs {
  command_id: string;
  stdout: string;
  stderr: string;
  truncated: boolean;
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
  disk_free_bytes: number | null;
  disk_total_bytes: number | null;
  disk_pressure: boolean;
  disk_limit_enforced: boolean;
}

export interface WorkerView {
  id: string;
  short_id: string;
  pool_id: string;
  pool_name: string;
  state: WorkerState;
  cloud: CloudProvider | null;
  region: string | null;
  zone: string | null;
  instance_type: string | null;
  use_spot: boolean;
  hourly_cost: number | null;
  provider_cluster: string;
  provider_metadata: Record<string, unknown>;
  tunnel_port: number | null;
  capacity: WorkerCapacity;
  version: string | null;
  protocol_version: number | null;
  draining: boolean;
  terminate_when_empty: boolean;
  created_at: string;
  updated_at: string;
  healthy_at: string | null;
  last_heartbeat_at: string | null;
  idle_since: string | null;
  consecutive_failures: number;
  last_error: string | null;
  provision_seconds: number | null;
}

export interface CloudPolicy {
  providers: CloudProvider[];
  strategy: "cost" | "time";
  region: string | null;
  zone: string | null;
  instance_type: string | null;
}

export interface SandboxDefaults {
  image: string;
  cpus: number;
  memory_bytes: number;
  pids_limit: number;
  timeout_seconds: number;
  max_timeout_seconds: number;
  network: NetworkPolicy;
  workdir: string;
}

export interface WorkerPool {
  id: string;
  name: string;
  cloud_policy: CloudPolicy;
  worker_cpus: number;
  worker_memory_bytes: number;
  worker_disk_gb: number;
  worker_reserve: { cpus: number; memory_bytes: number };
  min_workers: number;
  max_workers: number;
  use_spot: boolean;
  worker_idle_ttl_seconds: number;
  max_sandboxes_per_worker: number | null;
  warm_slots: number;
  runtime: string;
  default_image: string;
  sandbox_defaults: SandboxDefaults;
  preload_images: string[];
  created_at: string;
  updated_at: string;
}

export interface Operation {
  id: string;
  type: string;
  status: OperationStatus;
  pool_id: string | null;
  worker_id: string | null;
  sandbox_id: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  result: Record<string, unknown>;
}

export interface DoctorCheck {
  name: string;
  status: "ok" | "warn" | "fail";
  message: string;
}

export interface DoctorReport {
  ok: boolean;
  checks: DoctorCheck[];
  provider: Record<string, unknown>;
}

export interface ApiErrorPayload {
  code: string;
  message: string;
  hint?: string | null;
  details?: Record<string, unknown>;
}
