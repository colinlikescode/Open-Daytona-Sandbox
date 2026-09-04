"""Prometheus metrics. Labels are low-cardinality only (pool name, cloud, error class). Never sandbox ids."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.sandboxes_total = Counter(
            "sandboxpilot_sandboxes_total", "Sandboxes created", ["pool"], registry=r
        )
        self.sandboxes_running = Gauge(
            "sandboxpilot_sandboxes_running", "Sandboxes currently running", ["pool"], registry=r
        )
        self.sandbox_create_seconds = Histogram(
            "sandboxpilot_sandbox_create_seconds",
            "Create request to RUNNING (excludes worker provisioning)",
            ["pool"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.sandbox_errors_total = Counter(
            "sandboxpilot_sandbox_errors_total", "Sandbox errors", ["pool", "error"], registry=r
        )

        self.workers_total = Gauge(
            "sandboxpilot_workers_total", "Workers not terminated", ["pool"], registry=r
        )
        self.workers_healthy = Gauge(
            "sandboxpilot_workers_healthy", "Healthy workers", ["pool"], registry=r
        )
        self.worker_capacity_cpu_millis = Gauge(
            "sandboxpilot_worker_capacity_cpu_millis",
            "Allocatable CPU (millis)",
            ["pool"],
            registry=r,
        )
        self.worker_allocated_cpu_millis = Gauge(
            "sandboxpilot_worker_allocated_cpu_millis",
            "Allocated CPU (millis)",
            ["pool"],
            registry=r,
        )
        self.worker_capacity_memory_bytes = Gauge(
            "sandboxpilot_worker_capacity_memory_bytes", "Allocatable memory", ["pool"], registry=r
        )
        self.worker_allocated_memory_bytes = Gauge(
            "sandboxpilot_worker_allocated_memory_bytes", "Allocated memory", ["pool"], registry=r
        )
        self.worker_provision_seconds = Histogram(
            "sandboxpilot_worker_provision_seconds",
            "SkyPilot request to worker HEALTHY",
            ["pool", "cloud"],
            buckets=(30, 60, 120, 180, 300, 600, 900, 1800),
            registry=r,
        )
        self.worker_errors_total = Counter(
            "sandboxpilot_worker_errors_total", "Worker errors", ["pool", "error"], registry=r
        )

        self.command_duration_seconds = Histogram(
            "sandboxpilot_command_duration_seconds",
            "Command wall time",
            ["pool"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )
        self.command_errors_total = Counter(
            "sandboxpilot_command_errors_total", "Command errors", ["pool", "error"], registry=r
        )

        self.proxy_requests_total = Counter(
            "sandboxpilot_proxy_requests_total",
            "Proxied HTTP requests",
            ["method", "status"],
            registry=r,
        )
        self.proxy_request_duration_seconds = Histogram(
            "sandboxpilot_proxy_request_duration_seconds",
            "Proxy latency",
            ["method"],
            buckets=LATENCY_BUCKETS,
            registry=r,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)
