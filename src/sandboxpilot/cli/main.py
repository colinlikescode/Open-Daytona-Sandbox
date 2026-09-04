"""The ``sandboxpilot`` command line.

Layout: one Typer sub-app per resource (pool, worker, sandbox, image, template)
plus top-level verbs (init, daemon, status, doctor, cleanup, bench). Every
command talks to the control plane through the SDK; nothing here reaches into
internals, so the CLI is also a living example of the SDK.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml

from sandboxpilot.cli._output import (
    Output,
    console,
    detail,
    emit,
    err_console,
    fail,
    fmt,
    human_bytes,
    ok,
    table,
    warn,
)
from sandboxpilot.errors import ExitCode, SandboxPilotError
from sandboxpilot.schemas.common import CloudProvider, NetworkPolicy
from sandboxpilot.schemas.pool import CloudPolicy, PoolCreateRequest, PoolUpdateRequest
from sandboxpilot.schemas.template import Template
from sandboxpilot.sdk import Sandbox, SandboxPilot
from sandboxpilot.utils.sizes import parse_bytes
from sandboxpilot.version import __version__

app = typer.Typer(
    name="sandboxpilot",
    help="Fast gVisor sandboxes on SkyPilot-managed VMs in your own cloud.",
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)
pool_app = typer.Typer(
    help="Worker pools (a pool = one cloud config + autoscaling rules).", no_args_is_help=True
)
worker_app = typer.Typer(help="Worker VMs.", no_args_is_help=True)
sandbox_app = typer.Typer(help="Sandboxes.", no_args_is_help=True)
image_app = typer.Typer(help="Container images on workers.", no_args_is_help=True)
template_app = typer.Typer(help="Reusable sandbox templates.", no_args_is_help=True)
daemon_app = typer.Typer(help="The local control plane process.", no_args_is_help=True)
app.add_typer(pool_app, name="pool")
app.add_typer(worker_app, name="worker")
app.add_typer(sandbox_app, name="sandbox")
app.add_typer(image_app, name="image")
app.add_typer(template_app, name="template")
app.add_typer(daemon_app, name="daemon")

JsonFlag = Annotated[bool, typer.Option("--json", help="Print JSON instead of a table.")]


def _client(autostart: bool = True) -> SandboxPilot:
    return SandboxPilot(autostart=autostart)


def _kv(pairs: list[str] | None, what: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in pairs or []:
        if "=" not in item:
            raise typer.BadParameter(f"{what} must look like KEY=VALUE, got {item!r}")
        k, v = item.split("=", 1)
        out[k] = v
    return out


def _version(value: bool) -> None:
    if value:
        print(f"sandboxpilot {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    json_output: JsonFlag = False,
    version: Annotated[
        bool,
        typer.Option("--version", "-V", callback=_version, is_eager=True, help="Show version."),
    ] = False,
) -> None:
    Output.json_mode = json_output


# ------------------------------------------------------------------------------------------ init


@app.command()
def init(
    cloud: Annotated[
        str, typer.Option(help="aws, gcp, azure, auto or fake (local testing).")
    ] = "auto",
    image: Annotated[str, typer.Option(help="Default sandbox image.")] = "python:3.12-slim",
    force: Annotated[bool, typer.Option("--force", help="Overwrite an existing config.")] = False,
) -> None:
    """Write a starter config file and check cloud credentials."""
    from sandboxpilot.utils.paths import config_file, ensure_private_dir

    path = config_file()
    if path.exists() and not force:
        fail(f"{path} already exists", hint="Pass --force to overwrite it.")
        raise typer.Exit(ExitCode.CONFIGURATION_ERROR)
    providers = "auto" if cloud in {"auto", "fake"} else [cloud]
    cfg: dict[str, Any] = {
        "provider": {"type": "fake" if cloud == "fake" else "skypilot"},
        "pools": {
            "default": {
                "cloud": {"providers": providers, "strategy": "cost"},
                "workers": {"cpus": 8, "memory": "32GB", "disk": "100GB", "spot": False},
                "scaling": {"min_workers": 0, "max_workers": 5, "idle_ttl": "15m"},
                "sandbox": {"image": image, "cpus": 1, "memory": "2GB", "timeout": "1h"},
            }
        },
    }
    if cloud == "fake":
        cfg["pools"]["default"]["runtime"] = {"type": "fake"}
    ensure_private_dir(path.parent)
    path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    ok(f"wrote {path}")
    if cloud != "fake":
        _check_clouds()


def _check_clouds() -> None:
    try:
        from sandboxpilot.providers.skypilot import SkyPilotComputeProvider

        result = asyncio.run(SkyPilotComputeProvider().doctor())
    except Exception as exc:
        warn(f"could not check cloud credentials through SkyPilot: {exc}")
        warn("run `sky check` to see what is wrong")
        return
    if result.enabled_clouds:
        ok("clouds SkyPilot can use: " + ", ".join(sorted(c.value for c in result.enabled_clouds)))
    else:
        warn("SkyPilot found no usable cloud credentials. Run `sky check` for details.")
    for msg in result.errors + result.hints:
        warn(msg)


# ------------------------------------------------------------------------------------------ daemon


@daemon_app.command("run")
def daemon_run(
    config: Annotated[Path | None, typer.Option(help="Config file path.")] = None,
    host: Annotated[str | None, typer.Option(help="Bind address override.")] = None,
    port: Annotated[int | None, typer.Option(help="Port override.")] = None,
) -> None:
    """Run the control plane in the foreground."""
    from sandboxpilot.config.loader import load_config
    from sandboxpilot.control.daemon import serve

    overrides: dict[str, Any] = {}
    if host:
        overrides.setdefault("api", {})["host"] = host
    if port:
        overrides.setdefault("api", {})["port"] = port
    cfg = load_config(config, overrides=overrides)
    asyncio.run(serve(cfg))


@daemon_app.command("start")
def daemon_start() -> None:
    """Start the control plane in the background (if it is not already running)."""
    from sandboxpilot.config.loader import load_config
    from sandboxpilot.control.daemon import ensure_running

    url = ensure_running(load_config())
    ok(f"control plane is running at {url}")


@daemon_app.command("stop")
def daemon_stop() -> None:
    """Stop the background control plane. Workers and sandboxes are left alone."""
    from sandboxpilot.control.daemon import stop

    ok("stopped" if stop() else "not running")


@daemon_app.command("status")
def daemon_status() -> None:
    """Is the control plane up?"""
    from sandboxpilot.config.loader import api_url_from_env, load_config
    from sandboxpilot.control.daemon import is_healthy, pid_alive, read_pid
    from sandboxpilot.utils.paths import daemon_log_file

    url = api_url_from_env(load_config().api.url)
    pid = read_pid()
    info = {
        "url": url,
        "healthy": is_healthy(url),
        "pid": pid if pid and pid_alive(pid) else None,
        "log": str(daemon_log_file()),
    }
    detail(info)


@daemon_app.command("logs")
def daemon_logs(lines: Annotated[int, typer.Option("-n", help="Lines to show.")] = 100) -> None:
    """Show the tail of the daemon log."""
    from sandboxpilot.utils.paths import daemon_log_file

    path = daemon_log_file()
    if not path.exists():
        warn(f"no log file at {path}")
        return
    print("".join(path.read_text(errors="replace").splitlines(keepends=True)[-lines:]), end="")


# ------------------------------------------------------------------------------------------ system


@app.command()
def status() -> None:
    """Pools, workers, sandboxes, and hourly cost at a glance."""
    with _client() as c:
        st = c.status()
    if Output.json_mode:
        emit(st)
        return
    sbx = st.get("sandboxes", {})
    console.print(
        f"[bold]SandboxPilot {st.get('version')}[/bold]  provider={st.get('provider')}  "
        f"sandboxes running={sbx.get('running', 0)}"
    )
    table(
        [
            {
                "name": p["name"],
                "cloud": p["cloud"],
                "workers": f"{p['workers']['healthy']}/{p['workers']['total']}",
                "scaling": f"{p['workers']['min']}/{p['workers']['max']}",
                "sandboxes": p["sandboxes_running"],
                "cost": f"${p['estimated_hourly_cost']:.2f}/h",
            }
            for p in st.get("pools", [])
        ],
        [
            ("name", "POOL"),
            ("cloud", "CLOUD"),
            ("workers", "WORKERS"),
            ("scaling", "MIN/MAX"),
            ("sandboxes", "SANDBOXES"),
            ("cost", "COST"),
        ],
    )
    if st.get("workers"):
        from sandboxpilot.schemas.worker import WorkerView

        table([_worker_row(WorkerView.model_validate(w)) for w in st["workers"]], WORKER_COLUMNS)


@app.command()
def doctor() -> None:
    """Check config, cloud credentials, workers and gVisor health."""
    with _client() as c:
        report = c.doctor()
    if Output.json_mode:
        emit(report)
    else:
        for check in report.get("checks", []):
            icon = {"ok": "[green]✓[/green]", "warn": "[yellow]![/yellow]"}.get(
                check["status"], "[red]✗[/red]"
            )
            console.print(f"{icon} {check['name']}: {check['message']}")
    if not report.get("ok", True):
        raise typer.Exit(ExitCode.GENERIC_ERROR)


@app.command()
def cleanup(
    terminate_workers: Annotated[
        bool, typer.Option("--workers", help="Also terminate all workers.")
    ] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Do not ask.")] = False,
) -> None:
    """Clear out stale state. With --workers, terminate every worker (and its sandboxes)."""
    if (
        not yes
        and terminate_workers
        and not typer.confirm("Terminate all workers and their sandboxes?")
    ):
        raise typer.Exit()
    with _client() as c:
        detail(c.cleanup(terminate_workers=terminate_workers))


# ------------------------------------------------------------------------------------------ pools

POOL_COLUMNS = [
    ("name", "NAME"),
    ("providers", "CLOUDS"),
    ("scaling", "MIN/MAX"),
    ("worker_size", "WORKER SIZE"),
    ("image", "DEFAULT IMAGE"),
    ("warm", "WARM SLOTS"),
]


def _pool_row(p: Any) -> dict[str, Any]:
    prov = p.cloud_policy.providers
    return {
        "name": p.name,
        "providers": prov if isinstance(prov, str) else ",".join(x.value for x in prov),
        "scaling": f"{p.min_workers}/{p.max_workers}",
        "worker_size": f"{p.worker_cpus}c/{human_bytes(p.worker_memory_bytes)}"
        + (" spot" if p.use_spot else ""),
        "image": p.sandbox_defaults.image,
        "warm": p.warm_slots,
    }


@pool_app.command("list")
def pool_list() -> None:
    """List pools."""
    with _client() as c:
        pools = c.list_pools()
    table(
        [_pool_row(p) for p in pools], POOL_COLUMNS, title="Pools"
    ) if not Output.json_mode else emit(pools)


@pool_app.command("get")
def pool_get(name: str) -> None:
    """Show one pool."""
    with _client() as c:
        detail(c.get_pool(name))


@pool_app.command("create")
def pool_create(
    name: str,
    cloud: Annotated[
        list[str] | None,
        typer.Option("--cloud", help="aws, gcp or azure (repeatable). Default: auto."),
    ] = None,
    region: Annotated[str | None, typer.Option()] = None,
    instance_type: Annotated[str | None, typer.Option()] = None,
    cpus: Annotated[int | None, typer.Option(help="Worker vCPUs.")] = None,
    memory: Annotated[str | None, typer.Option(help="Worker memory, e.g. 32GB.")] = None,
    disk: Annotated[str | None, typer.Option(help="Worker disk, e.g. 100GB.")] = None,
    spot: Annotated[bool, typer.Option("--spot", help="Use spot/preemptible VMs.")] = False,
    min_workers: Annotated[int | None, typer.Option()] = None,
    max_workers: Annotated[int | None, typer.Option()] = None,
    idle_ttl: Annotated[
        str | None, typer.Option(help="Terminate idle workers after, e.g. 15m.")
    ] = None,
    image: Annotated[str | None, typer.Option(help="Default sandbox image.")] = None,
    warm_slots: Annotated[int | None, typer.Option(help="Pre-booted sandboxes per worker.")] = None,
    runtime: Annotated[str | None, typer.Option(help="gvisor (default) or fake.")] = None,
) -> None:
    """Create a pool."""
    policy = None
    if cloud or region or instance_type:
        policy = CloudPolicy(
            providers=[CloudProvider(c) for c in cloud] if cloud else "auto",
            region=region,
            instance_type=instance_type,
        )
    req = PoolCreateRequest(
        name=name,
        cloud=policy,
        worker_cpus=cpus,
        worker_memory=memory,
        worker_disk_gb=max(1, parse_bytes(disk) // 1000**3) if disk else None,
        use_spot=spot or None,
        min_workers=min_workers,
        max_workers=max_workers,
        idle_ttl=idle_ttl,
        default_image=image,
        warm_slots=warm_slots,
        runtime=runtime,
    )
    with _client() as c:
        pool = c.create_pool(req)
    ok(f"created pool {pool.name}") if not Output.json_mode else emit(pool)


@pool_app.command("update")
def pool_update(
    name: str,
    min_workers: Annotated[int | None, typer.Option()] = None,
    max_workers: Annotated[int | None, typer.Option()] = None,
    idle_ttl: Annotated[str | None, typer.Option()] = None,
    warm_slots: Annotated[int | None, typer.Option()] = None,
    image: Annotated[str | None, typer.Option(help="Default sandbox image.")] = None,
) -> None:
    """Change scaling or defaults on a pool."""
    req = PoolUpdateRequest(
        min_workers=min_workers, max_workers=max_workers, idle_ttl=idle_ttl, warm_slots=warm_slots
    )
    if image:
        req.default_image = image
    with _client() as c:
        detail(c.update_pool(name, req))


@pool_app.command("delete")
def pool_delete(
    name: str,
    force: Annotated[
        bool, typer.Option("--force", help="Kill sandboxes and workers first.")
    ] = False,
) -> None:
    """Delete a pool (must be empty unless --force)."""
    with _client() as c:
        c.delete_pool(name, force=force)
    ok(f"deleted pool {name}")


@pool_app.command("up")
def pool_up(
    name: str = "default",
    workers: Annotated[
        int | None, typer.Option(help="How many workers to bring up (default: min_workers or 1).")
    ] = None,
    wait: Annotated[
        bool, typer.Option("--wait/--no-wait", help="Wait for workers to be READY.")
    ] = True,
) -> None:
    """Provision workers so the first sandbox is fast."""
    with _client() as c:
        ops = c.pool_up(name, workers=workers)
        if not ops:
            ok("pool already has enough workers")
            return
        ok(f"provisioning {len(ops)} worker(s) through SkyPilot; this takes a few minutes")
        if wait:
            for op in ops:
                done = c.wait_operation(op.id)
                ok(f"worker {done.worker_id} ready")
        else:
            emit([o.id for o in ops]) if Output.json_mode else None


@pool_app.command("down")
def pool_down(
    name: str = "default",
    force: Annotated[bool, typer.Option("--force", help="Kill running sandboxes too.")] = False,
) -> None:
    """Terminate all workers in a pool."""
    with _client() as c:
        detail(c.pool_down(name, force=force))


# ------------------------------------------------------------------------------------------ workers

WORKER_COLUMNS = [
    ("short_id", "ID"),
    ("pool", "POOL"),
    ("state", "STATE"),
    ("cloud", "CLOUD"),
    ("instance", "INSTANCE"),
    ("sandboxes", "SANDBOXES"),
    ("free", "FREE"),
    ("cost", "$/H"),
]


def _worker_row(w: Any) -> dict[str, Any]:
    cap = w.capacity
    return {
        "short_id": w.short_id,
        "pool": w.pool_name,
        "state": w.state.value + (" (draining)" if w.draining else ""),
        "cloud": f"{w.cloud.value if w.cloud else '-'}/{w.region or '-'}",
        "instance": (w.instance_type or "-") + (" spot" if w.use_spot else ""),
        "sandboxes": f"{cap.sandboxes_running}"
        + (f" +{cap.sandboxes_warm} warm" if cap.sandboxes_warm else ""),
        "free": f"{(cap.cpu_millis_allocatable - cap.cpu_millis_allocated) / 1000:.1f}c/"
        f"{human_bytes(cap.memory_bytes_allocatable - cap.memory_bytes_allocated)}",
        "cost": f"{w.hourly_cost:.2f}" if w.hourly_cost is not None else "-",
    }


@worker_app.command("list")
def worker_list(pool: Annotated[str | None, typer.Option()] = None) -> None:
    """List workers."""
    with _client() as c:
        workers = c.list_workers(pool)
    table(
        [_worker_row(w) for w in workers], WORKER_COLUMNS, title="Workers"
    ) if not Output.json_mode else emit(workers)


@worker_app.command("get")
def worker_get(worker_id: str) -> None:
    """Show one worker."""
    with _client() as c:
        detail(c.get_worker(worker_id))


@worker_app.command("drain")
def worker_drain(worker_id: str) -> None:
    """Stop scheduling onto a worker; terminate it once its sandboxes finish."""
    with _client() as c:
        detail(c.drain_worker(worker_id))


@worker_app.command("remove")
def worker_remove(
    worker_id: str,
    force: Annotated[bool, typer.Option("--force", help="Kill its sandboxes too.")] = False,
) -> None:
    """Terminate a worker VM."""
    with _client() as c:
        detail(c.remove_worker(worker_id, force=force))


# ------------------------------------------------------------------------------------------ sandboxes

SANDBOX_COLUMNS = [
    ("short_id", "ID"),
    ("state", "STATE"),
    ("image", "IMAGE"),
    ("size", "SIZE"),
    ("pool", "POOL"),
    ("worker", "WORKER"),
    ("expires", "EXPIRES"),
]


def _sandbox_row(s: Any) -> dict[str, Any]:
    return {
        "short_id": s.short_id,
        "state": s.state.value,
        "image": s.image,
        "size": f"{s.cpus:g}c/{human_bytes(s.memory_bytes)}",
        "pool": s.pool,
        "worker": (s.worker_id or "-")[-12:],
        "expires": fmt(s.expires_at),
    }


@sandbox_app.command("list")
def sandbox_list(
    pool: Annotated[str | None, typer.Option()] = None,
    all_states: Annotated[
        bool, typer.Option("--all", "-a", help="Include stopped/failed.")
    ] = False,
) -> None:
    """List sandboxes."""
    with _client() as c:
        boxes = c.list_sandboxes(pool=pool, all=all_states)
    table(
        [_sandbox_row(s) for s in boxes], SANDBOX_COLUMNS, title="Sandboxes"
    ) if not Output.json_mode else emit(boxes)


@sandbox_app.command("get")
def sandbox_get(sandbox_id: str) -> None:
    """Show one sandbox."""
    with _client() as c:
        detail(c.get_sandbox_info(sandbox_id))


@sandbox_app.command("create")
def sandbox_create(
    image: Annotated[str | None, typer.Option("--image", "-i")] = None,
    pool: Annotated[str | None, typer.Option()] = None,
    template: Annotated[str | None, typer.Option("--template", "-t")] = None,
    cpus: Annotated[float | None, typer.Option()] = None,
    memory: Annotated[str | None, typer.Option(help="e.g. 2GB")] = None,
    timeout: Annotated[str | None, typer.Option(help="Auto-kill after, e.g. 30m")] = None,
    env: Annotated[
        list[str] | None, typer.Option("--env", "-e", help="KEY=VALUE (repeatable)")
    ] = None,
    network: Annotated[str | None, typer.Option(help="internet or none")] = None,
    workdir: Annotated[str | None, typer.Option()] = None,
    label: Annotated[
        list[str] | None, typer.Option("--label", "-l", help="KEY=VALUE (repeatable)")
    ] = None,
) -> None:
    """Create a sandbox and print its id."""
    sb = Sandbox.create(
        image,
        pool=pool,
        template=template,
        cpus=cpus,
        memory=memory,
        timeout=timeout,
        env=_kv(env, "--env"),
        workdir=workdir,
        network=NetworkPolicy(network) if network else None,
        labels=_kv(label, "--label"),
    )
    try:
        if Output.json_mode:
            emit(sb.info)
        else:
            # Only the id goes to stdout so `SB=$(sandboxpilot sandbox create)` works.
            print(sb.id)
            ms = sb.info.metrics.get("sandbox_start_seconds")
            if ms is not None:
                warm = ", warm slot" if sb.info.metrics.get("warm_slot") else ""
                err_console.print(f"[dim]started in {ms * 1000:.0f} ms{warm}[/dim]")
    finally:
        sb._client.close()


@sandbox_app.command("exec")
def sandbox_exec(
    sandbox_id: str,
    command: Annotated[
        list[str], typer.Argument(help="Command. Use -- before flags meant for it.")
    ],
    cwd: Annotated[str | None, typer.Option()] = None,
    env: Annotated[list[str] | None, typer.Option("--env", "-e")] = None,
    timeout: Annotated[float | None, typer.Option(help="Seconds")] = None,
    stream: Annotated[
        bool, typer.Option("--stream/--no-stream", help="Stream output as it happens.")
    ] = True,
) -> None:
    """Run a command in a sandbox; exit with its exit code."""
    argv: str | list[str] = command[0] if len(command) == 1 else command
    sb = Sandbox.connect(sandbox_id)
    try:
        if stream and not Output.json_mode:
            code = 0
            for ev in sb.stream(argv, cwd=cwd, env=_kv(env, "--env"), timeout=timeout):
                if ev.type == "stdout" and ev.text:
                    sys.stdout.write(ev.text)
                    sys.stdout.flush()
                elif ev.type == "stderr" and ev.text:
                    sys.stderr.write(ev.text)
                    sys.stderr.flush()
                elif ev.type == "exit":
                    code = ev.exit_code or 0
            raise typer.Exit(code)
        result = sb.run(argv, cwd=cwd, env=_kv(env, "--env"), timeout=timeout)
        if Output.json_mode:
            emit(result)
        else:
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
        raise typer.Exit(result.exit_code or 0)
    finally:
        sb._client.close()


@sandbox_app.command("kill")
def sandbox_kill(sandbox_ids: list[str]) -> None:
    """Kill one or more sandboxes."""
    with _client() as c:
        for sid in sandbox_ids:
            c.kill_sandbox(sid)
            ok(f"killed {sid}")


@sandbox_app.command("timeout")
def sandbox_timeout(
    sandbox_id: str, timeout: Annotated[str, typer.Argument(help="e.g. 30m")]
) -> None:
    """Change when a sandbox auto-kills."""
    sb = Sandbox.connect(sandbox_id)
    try:
        detail(sb.set_timeout(timeout), keys=["id", "expires_at"])
    finally:
        sb._client.close()


@sandbox_app.command("upload")
def sandbox_upload(sandbox_id: str, local: Path, remote: str) -> None:
    """Copy a local file or directory into the sandbox."""
    sb = Sandbox.connect(sandbox_id)
    try:
        sb.upload(local, remote)
        ok(f"uploaded {local} -> {remote}")
    finally:
        sb._client.close()


@sandbox_app.command("download")
def sandbox_download(sandbox_id: str, remote: str, local: Path) -> None:
    """Copy a file out of the sandbox."""
    sb = Sandbox.connect(sandbox_id)
    try:
        dest = sb.download(remote, local)
        ok(f"downloaded {remote} -> {dest}")
    finally:
        sb._client.close()


@sandbox_app.command("url")
def sandbox_url(
    sandbox_id: str,
    port: int,
    expires_in: Annotated[int | None, typer.Option(help="Seconds")] = None,
) -> None:
    """Get a signed URL that proxies to a port inside the sandbox."""
    sb = Sandbox.connect(sandbox_id)
    try:
        print(sb.get_url(port, expires_in=expires_in))
    finally:
        sb._client.close()


# ------------------------------------------------------------------------------------------ images / templates


@image_app.command("preload")
def image_preload(
    reference: str,
    pool: Annotated[str | None, typer.Option()] = None,
    wait: Annotated[bool, typer.Option("--wait/--no-wait")] = True,
) -> None:
    """Pull an image onto every ready worker in a pool."""
    with _client() as c:
        op = c.preload_image(reference, pool=pool)
        if wait:
            op = c.wait_operation(op.id)
        detail(op, keys=["id", "status", "result", "error"])


@image_app.command("list")
def image_list(worker: Annotated[str | None, typer.Option()] = None) -> None:
    """Images present on workers."""
    with _client() as c:
        rows = c.list_images(worker=worker)
    table(
        rows,
        [("reference", "IMAGE"), ("worker_id", "WORKER"), ("size_bytes", "SIZE")],
        title="Images",
    )


@template_app.command("list")
def template_list() -> None:
    """List templates."""
    with _client() as c:
        tpls = c.list_templates()
    table(
        [
            {
                "name": t.name,
                "image": t.image,
                "cpus": t.resources.cpus,
                "memory": t.resources.memory,
                "description": t.description,
            }
            for t in tpls
        ],
        [
            ("name", "NAME"),
            ("image", "IMAGE"),
            ("cpus", "CPUS"),
            ("memory", "MEMORY"),
            ("description", "DESCRIPTION"),
        ],
        title="Templates",
    ) if not Output.json_mode else emit(tpls)


@template_app.command("add")
def template_add(
    file: Annotated[
        Path | None, typer.Option("--file", "-f", help="YAML/JSON template file.")
    ] = None,
    name: Annotated[str | None, typer.Option()] = None,
    image: Annotated[str | None, typer.Option()] = None,
    cpus: Annotated[float | None, typer.Option()] = None,
    memory: Annotated[str | None, typer.Option()] = None,
    env: Annotated[list[str] | None, typer.Option("--env", "-e")] = None,
) -> None:
    """Add a template from a file or from flags."""
    if file:
        data = yaml.safe_load(file.read_text()) or {}
    else:
        if not name:
            raise typer.BadParameter("--name is required without --file")
        data = {"name": name, "image": image, "env": _kv(env, "--env")}
        if cpus or memory:
            data["resources"] = {"cpus": cpus, "memory": memory}
    with _client() as c:
        tpl = c.add_template(Template.model_validate(data))
    ok(f"added template {tpl.name}") if not Output.json_mode else emit(tpl)


@template_app.command("remove")
def template_remove(name: str) -> None:
    """Remove a template."""
    with _client() as c:
        c.remove_template(name)
    ok(f"removed template {name}")


# ------------------------------------------------------------------------------------------ bench


@app.command()
def bench(
    iterations: Annotated[int, typer.Option("-n", help="Sandboxes to create per scenario.")] = 10,
    image: Annotated[str | None, typer.Option()] = None,
    pool: Annotated[str | None, typer.Option()] = None,
    concurrency: Annotated[
        int, typer.Option("-c", help="Parallel sandboxes for the concurrency scenario.")
    ] = 5,
    out: Annotated[Path | None, typer.Option(help="Write JSON results here.")] = None,
) -> None:
    """Measure warm/cold sandbox start, command latency and provisioning time."""
    from sandboxpilot.bench.runner import run_benchmark

    report = run_benchmark(iterations=iterations, image=image, pool=pool, concurrency=concurrency)
    if out:
        out.write_text(json.dumps(report, indent=2, default=str))
    if Output.json_mode:
        emit(report)
        return
    for name, stats in report["scenarios"].items():
        console.print(
            f"[bold]{name}[/bold]  n={stats['n']}  p50={stats['p50_ms']:.0f}ms  p95={stats['p95_ms']:.0f}ms  max={stats['max_ms']:.0f}ms"
        )
    if report.get("notes"):
        for note in report["notes"]:
            console.print(f"  note: {note}")


# ------------------------------------------------------------------------------------------ shortcuts
# The README teaches these first. They are thin aliases over the resource commands.


@app.command()
def up(
    cloud: Annotated[
        str | None, typer.Option(help="Pin the default pool to aws, gcp or azure.")
    ] = None,
    workers: Annotated[int | None, typer.Option(help="How many workers to start.")] = None,
    pool: str = "default",
    wait: Annotated[bool, typer.Option("--wait/--no-wait")] = True,
) -> None:
    """Start warm workers so the first sandbox is fast."""
    if cloud:
        with _client() as c:
            current = c.get_pool(pool)
            policy = current.cloud_policy.model_copy(update={"providers": [CloudProvider(cloud)]})
            c.update_pool(pool, PoolUpdateRequest(cloud=policy))
    pool_up(pool, workers=workers, wait=wait)


@app.command()
def down(
    pool: str = "default",
    force: Annotated[bool, typer.Option("--force", help="Kill running sandboxes too.")] = True,
) -> None:
    """Terminate all workers. Billing stops."""
    pool_down(pool, force=force)


@app.command()
def run(
    command: Annotated[list[str], typer.Argument(help="Command to run in a fresh sandbox.")],
    image: Annotated[str | None, typer.Option("--image", "-i")] = None,
    pool: Annotated[str | None, typer.Option()] = None,
    env: Annotated[list[str] | None, typer.Option("--env", "-e")] = None,
    timeout: Annotated[float | None, typer.Option(help="Command timeout in seconds")] = None,
) -> None:
    """Run a command in a brand new sandbox, print its output, kill the sandbox."""
    argv: str | list[str] = command[0] if len(command) == 1 else command
    with Sandbox.create(image, pool=pool, env=_kv(env, "--env"), timeout="30m") as sb:
        code = 0
        for ev in sb.stream(argv, timeout=timeout):
            if ev.type == "stdout":
                sys.stdout.write(ev.text)
                sys.stdout.flush()
            elif ev.type == "stderr":
                sys.stderr.write(ev.text)
                sys.stderr.flush()
            elif ev.type == "exit":
                code = ev.exit_code or 0
    raise typer.Exit(code)


app.command("create")(sandbox_create)
app.command("exec")(sandbox_exec)
app.command("kill")(sandbox_kill)


@app.command()
def cp(src: str, dst: str) -> None:
    """Copy files in or out: `cp ./file <id>:/path` or `cp <id>:/path ./file`."""
    if ":" in dst and not Path(dst.split(":", 1)[0]).exists():
        sid, remote = dst.split(":", 1)
        sandbox_upload(sid, Path(src), remote)
    elif ":" in src:
        sid, remote = src.split(":", 1)
        sandbox_download(sid, remote, Path(dst))
    else:
        raise typer.BadParameter("one side must look like <sandbox-id>:<path>")


# ------------------------------------------------------------------------------------------ entry


def main() -> None:
    try:
        app(prog_name="sandboxpilot")
    except SandboxPilotError as exc:
        fail(exc.message, getattr(exc, "hint", None))
        sys.exit(int(exc.exit_code))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    if os.environ.get("SANDBOXPILOT_DEBUG"):
        app(prog_name="sandboxpilot")
    else:
        main()
