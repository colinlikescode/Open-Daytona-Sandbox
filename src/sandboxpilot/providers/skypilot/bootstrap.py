"""Build the SkyPilot task payload that bootstraps a VM into a SandboxPilot worker.

Secrets (the worker token) travel via a 0600 file mount, never via command
lines or task environment variables that could end up in logs.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from sandboxpilot.config import defaults as d
from sandboxpilot.errors import WorkerProvisionError
from sandboxpilot.providers.base import WorkerProvisionRequest
from sandboxpilot.utils.logging import get_logger

log = get_logger("providers.skypilot.bootstrap")

SCRIPTS_DIR = Path(__file__).parent / "scripts"
REMOTE_STAGE_DIR = "/tmp/sandboxpilot"
INSTALL_MODE_ENV = "SANDBOXPILOT_WORKER_INSTALL"


@dataclass
class BootstrapPayload:
    setup: str
    run: str
    file_mounts: dict[str, str]
    envs: dict[str, str]
    staging_dir: Path
    install_mode: str

    def cleanup(self) -> None:
        shutil.rmtree(self.staging_dir, ignore_errors=True)


def render_worker_env(request: WorkerProvisionRequest) -> str:
    pool = request.pool
    lines = [
        f"SANDBOXPILOT_WORKER_ID={request.worker_id}",
        f"SANDBOXPILOT_WORKER_POOL_ID={pool.id}",
        f"SANDBOXPILOT_WORKER_TOKEN={request.worker_token}",
        f"SANDBOXPILOT_WORKER_RUNTIME={pool.runtime}",
        f"SANDBOXPILOT_WORKER_HOST={d.WORKER_HOST}",
        f"SANDBOXPILOT_WORKER_PORT={d.WORKER_PORT}",
        f"SANDBOXPILOT_WORKER_RESERVE_CPUS={pool.worker_reserve.cpus}",
        f"SANDBOXPILOT_WORKER_RESERVE_MEMORY={pool.worker_reserve.memory_bytes}",
        f"SANDBOXPILOT_WORKER_PRELOAD_IMAGES={','.join(request.preload_images)}",
        f"SANDBOXPILOT_WORKER_NETWORK_NAME={d.SANDBOX_NETWORK_NAME}",
        f"SANDBOXPILOT_WORKER_NETWORK_SUBNET={d.SANDBOX_NETWORK_SUBNET}",
    ]
    if pool.max_sandboxes_per_worker:
        lines.append(f"SANDBOXPILOT_WORKER_MAX_SANDBOXES={pool.max_sandboxes_per_worker}")
    if pool.warm_slots > 0:
        lines.append(f"SANDBOXPILOT_WORKER_WARM_SLOTS={pool.warm_slots}")
        # base64 so the JSON survives `bash` sourcing and systemd's EnvironmentFile parser.
        spec_b64 = base64.b64encode(pool.warm_spec().model_dump_json().encode()).decode()
        lines.append(f"SANDBOXPILOT_WORKER_WARM_SPEC={spec_b64}")
    if pool.runtime == "docker-unsafe":
        lines.append("SANDBOXPILOT_DEV_UNSAFE_RUNTIME=1")
    return "\n".join(lines) + "\n"


def detect_install_mode() -> str:
    """``local`` when running from a source checkout (build a wheel), else ``release``."""
    forced = os.environ.get(INSTALL_MODE_ENV)
    if forced in {"local", "release"}:
        return forced
    import sandboxpilot

    package_dir = Path(sandboxpilot.__file__).resolve().parent
    repo_root = package_dir.parent.parent
    if (repo_root / "pyproject.toml").exists() and (repo_root / "src" / "sandboxpilot").exists():
        return "local"
    return "release"


def build_wheel(dest: Path) -> Path:
    import sandboxpilot

    repo_root = Path(sandboxpilot.__file__).resolve().parent.parent.parent
    dest.mkdir(parents=True, exist_ok=True)
    log.info("building local wheel for worker install")
    # Try whatever build tool is around: uv, then `build`, then plain pip.
    attempts: list[list[str]] = []
    if uv := shutil.which("uv"):
        attempts.append([uv, "build", "--wheel", "--out-dir", str(dest), str(repo_root)])
    attempts.append(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(dest), str(repo_root)]
    )
    attempts.append(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", str(dest), str(repo_root)]
    )
    proc: subprocess.CompletedProcess[str] | None = None
    for cmd in attempts:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode == 0:
            break
    if proc is None or proc.returncode != 0:
        raise WorkerProvisionError(
            "Could not build the SandboxPilot wheel for local worker installation.",
            hint="Install uv or `pip install build`, or set SANDBOXPILOT_WORKER_INSTALL=release.",
            details={"stderr": (proc.stderr[-2000:] if proc else "")},
        )
    wheels = sorted(dest.glob("sandboxpilot-*.whl"))
    if not wheels:
        raise WorkerProvisionError(
            "wheel build produced no artifact", details={"stdout": proc.stdout[-2000:]}
        )
    return wheels[-1]


def build_payload(
    request: WorkerProvisionRequest, *, install_mode: str | None = None
) -> BootstrapPayload:
    mode = install_mode or detect_install_mode()
    staging = Path(tempfile.mkdtemp(prefix="sandboxpilot-bootstrap-"))
    os.chmod(staging, 0o700)
    env_path = staging / "worker.env"
    env_path.write_text(render_worker_env(request))
    os.chmod(env_path, 0o600)
    for script in ("bootstrap-worker.sh", "install-gvisor.sh"):
        shutil.copy(SCRIPTS_DIR / script, staging / script)
    file_mounts = {
        f"{REMOTE_STAGE_DIR}/worker.env": str(env_path),
        f"{REMOTE_STAGE_DIR}/bootstrap-worker.sh": str(staging / "bootstrap-worker.sh"),
        f"{REMOTE_STAGE_DIR}/install-gvisor.sh": str(staging / "install-gvisor.sh"),
    }
    envs = {
        "SP_STAGE_DIR": REMOTE_STAGE_DIR,
        "SP_INSTALL_MODE": mode,
        "SP_VERSION": request.sandboxpilot_version,
    }
    if mode == "local":
        wheel = build_wheel(staging / "wheels")
        file_mounts[f"{REMOTE_STAGE_DIR}/wheels/{wheel.name}"] = str(wheel)
    setup = f"bash {REMOTE_STAGE_DIR}/bootstrap-worker.sh"
    run = "echo sandboxpilot worker bootstrap complete"
    return BootstrapPayload(
        setup=setup,
        run=run,
        file_mounts=file_mounts,
        envs=envs,
        staging_dir=staging,
        install_mode=mode,
    )
