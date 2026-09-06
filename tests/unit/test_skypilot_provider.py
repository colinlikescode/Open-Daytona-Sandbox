"""SkyPilot provider against a fake `sky` module: no cloud, no SkyPilot install needed.

Covers the 0.9+ behaviour where ``launch`` returns as soon as the job is submitted
and the bootstrap keeps running as the job's setup.
"""

from __future__ import annotations

import io
from enum import Enum
from types import SimpleNamespace
from typing import Any

import pytest

from sandboxpilot.errors import WorkerProvisionError
from sandboxpilot.providers.skypilot.compatibility import SkyPilotCompatibility
from sandboxpilot.providers.skypilot.provider import (
    SkyPilotComputeProvider,
    _job_id_from_launch,
)


class JobStatus(Enum):
    SETTING_UP = "SETTING_UP"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_SETUP = "FAILED_SETUP"


class FakeSdk:
    """Mimics ``sky.client.sdk`` with the request-id API: calls return a token, ``get`` resolves."""

    def __init__(self) -> None:
        self.statuses: list[JobStatus] = []
        self.tail_exit: int | None | Exception = 0
        self.tail_text = "bootstrap output\nworker healthy\n"
        self.calls: list[str] = []
        self._results: dict[str, Any] = {}

    def _req(self, value: Any) -> str:
        rid = f"req-{len(self._results)}"
        self._results[rid] = value
        return rid

    def get(self, rid: str) -> Any:
        return self._results.pop(rid)

    def stream_and_get(self, rid: str) -> Any:
        return self.get(rid)

    def tail_logs(self, cluster: str, job_id: int, follow: bool, output_stream: io.StringIO) -> int:
        self.calls.append("tail_logs")
        if isinstance(self.tail_exit, Exception):
            raise self.tail_exit
        output_stream.write(self.tail_text)
        assert self.tail_exit is not None
        return self.tail_exit

    def job_status(self, cluster: str, job_ids: list[int]) -> str:
        self.calls.append("job_status")
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return self._req({job_ids[0]: status})


def make_provider(sdk: FakeSdk) -> SkyPilotComputeProvider:
    module = SimpleNamespace(
        client=SimpleNamespace(sdk=sdk), get=sdk.get, stream_and_get=sdk.stream_and_get
    )
    compat = SkyPilotCompatibility(
        module=module, version="0.13.0", version_tuple=(0, 13, 0), request_api=True
    )
    return SkyPilotComputeProvider(compat, bootstrap_timeout_seconds=5)


def test_job_id_extraction_is_lenient() -> None:
    assert _job_id_from_launch((1, object())) == 1
    assert _job_id_from_launch([7, None]) == 7
    assert _job_id_from_launch("3") == 3
    assert _job_id_from_launch((None, None)) is None
    assert _job_id_from_launch(None) is None
    assert _job_id_from_launch((True, None)) is None


def test_wait_for_bootstrap_follows_logs_until_success() -> None:
    sdk = FakeSdk()
    make_provider(sdk)._wait_for_bootstrap("sp-x", 1)
    assert sdk.calls == ["tail_logs"]


def test_wait_for_bootstrap_reports_setup_failure_with_log_tail() -> None:
    sdk = FakeSdk()
    sdk.tail_exit = 1
    sdk.tail_text = (
        "\n".join(f"line {i}" for i in range(60)) + "\nERROR: Docker does not expose runsc\n"
    )
    with pytest.raises(WorkerProvisionError) as info:
        make_provider(sdk)._wait_for_bootstrap("sp-x", 1)
    err = info.value
    assert "exit code 1" in err.message
    assert "sky logs sp-x" in (err.hint or "")
    tail = err.details["log_tail"]
    assert tail[-1] == "ERROR: Docker does not expose runsc"
    assert len(tail) == 40


def test_wait_for_bootstrap_falls_back_to_status_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = FakeSdk()
    sdk.tail_exit = ConnectionError("api server restarted")
    sdk.statuses = [JobStatus.SETTING_UP, JobStatus.RUNNING, JobStatus.SUCCEEDED]
    monkeypatch.setattr("sandboxpilot.providers.skypilot.provider.time.sleep", lambda _s: None)
    make_provider(sdk)._wait_for_bootstrap("sp-x", 1)
    assert sdk.calls[0] == "tail_logs" and sdk.calls.count("job_status") == 3


def test_wait_for_bootstrap_polling_detects_failed_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    sdk = FakeSdk()
    sdk.tail_exit = ConnectionError("gone")
    sdk.statuses = [JobStatus.FAILED_SETUP]
    monkeypatch.setattr("sandboxpilot.providers.skypilot.provider.time.sleep", lambda _s: None)
    with pytest.raises(WorkerProvisionError, match="bootstrap failed"):
        make_provider(sdk)._wait_for_bootstrap("sp-x", 1)


def test_wait_for_bootstrap_without_job_id_defers_to_health_check() -> None:
    sdk = FakeSdk()
    make_provider(sdk)._wait_for_bootstrap("sp-x", None)  # no exception, no calls
    assert sdk.calls == []
