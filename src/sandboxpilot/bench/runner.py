"""Benchmark runner behind ``sandboxpilot bench``.

Measures the numbers people actually ask about, each one separately so a slow
image pull is never mistaken for a slow sandbox start:

- warm_start:     create on a worker that already has the image (and, if the pool
                  has warm slots, a pre-booted sandbox to claim)
- cold_image:     create with an image no worker has yet (pull included)
- exec:           a trivial command in a running sandbox, round trip via the API
- concurrent:     N sandboxes created at once
- provision:      time for `pool up` to deliver a READY worker, if one had to be
                  created for this run (reported from the worker record)

Everything goes through the public SDK, so the client overhead is included in
every number, the same way a user would see it.
"""

from __future__ import annotations

import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sandboxpilot.sdk import Sandbox, SandboxPilot

COLD_IMAGE_CANDIDATES = ["busybox:1.36", "alpine:3.20", "debian:bookworm-slim"]


def _stats(samples_ms: list[float]) -> dict[str, float | int]:
    if not samples_ms:
        return {"n": 0, "p50_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}
    ordered = sorted(samples_ms)
    p95_index = min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))
    return {
        "n": len(ordered),
        "p50_ms": statistics.median(ordered),
        "p95_ms": ordered[p95_index],
        "max_ms": ordered[-1],
        "mean_ms": statistics.fmean(ordered),
    }


def _timed_create(
    client: SandboxPilot, image: str | None, pool: str | None
) -> tuple[float, Sandbox]:
    t0 = time.perf_counter()
    sb = Sandbox.create(image, pool=pool, timeout="10m", client=client)
    return (time.perf_counter() - t0) * 1000, sb


def run_benchmark(
    *,
    iterations: int = 10,
    image: str | None = None,
    pool: str | None = None,
    concurrency: int = 5,
    client: SandboxPilot | None = None,
) -> dict[str, Any]:
    owns = client is None
    client = client or SandboxPilot()
    notes: list[str] = []
    scenarios: dict[str, Any] = {}
    try:
        # First create also covers worker provisioning when the pool is empty.
        # Time it, but keep it out of the warm numbers.
        warm_up_ms, first = _timed_create(client, image, pool)
        info = first.info
        provision = None
        if info.worker_id:
            worker = client.get_worker(info.worker_id)
            provision = worker.provision_seconds
        scenarios["first_create"] = {
            "n": 1,
            "p50_ms": warm_up_ms,
            "p95_ms": warm_up_ms,
            "max_ms": warm_up_ms,
            "mean_ms": warm_up_ms,
        }
        if provision:
            scenarios["provision"] = {
                "n": 1,
                "p50_ms": provision * 1000,
                "p95_ms": provision * 1000,
                "max_ms": provision * 1000,
                "mean_ms": provision * 1000,
            }
            notes.append(
                "provision = time SkyPilot + bootstrap needed for the worker that served this run"
            )

        # exec latency
        exec_samples = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            first.run("true")
            exec_samples.append((time.perf_counter() - t0) * 1000)
        scenarios["exec"] = _stats(exec_samples)
        first.kill()

        # warm start: image present, worker ready
        warm_samples = []
        warm_slot_hits = 0
        for _ in range(iterations):
            ms, sb = _timed_create(client, image, pool)
            warm_samples.append(ms)
            if sb.info.metrics.get("warm_slot"):
                warm_slot_hits += 1
            sb.kill()
        scenarios["warm_start"] = _stats(warm_samples)
        if warm_slot_hits:
            notes.append(
                f"warm_start: {warm_slot_hits}/{iterations} creates claimed a pre-booted warm slot"
            )

        # cold image: pick an image no worker has
        present = {img.get("reference") for img in client.list_images()}
        cold = next((c for c in COLD_IMAGE_CANDIDATES if c not in present and c != image), None)
        if cold:
            ms, sb = _timed_create(client, cold, pool)
            pull = sb.info.metrics.get("image_pull_seconds")
            scenarios["cold_image"] = {
                "n": 1,
                "p50_ms": ms,
                "p95_ms": ms,
                "max_ms": ms,
                "mean_ms": ms,
            }
            if pull is not None:
                notes.append(
                    f"cold_image: {pull * 1000:.0f} ms of that was the image pull ({cold})"
                )
            sb.kill()
        else:
            notes.append("cold_image skipped: all candidate images already present")

        # concurrency
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            t0 = time.perf_counter()
            results = list(ex.map(lambda _: _timed_create(client, image, pool), range(concurrency)))
            wall = (time.perf_counter() - t0) * 1000
        scenarios["concurrent"] = _stats([ms for ms, _ in results])
        scenarios["concurrent"]["wall_ms"] = wall
        for _, sb in results:
            sb.kill()
    finally:
        if owns:
            client.close()
    return {
        "iterations": iterations,
        "image": image,
        "pool": pool,
        "scenarios": scenarios,
        "notes": notes,
    }
