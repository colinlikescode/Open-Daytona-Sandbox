"""SandboxPilot exception hierarchy and process exit codes."""

from __future__ import annotations

from typing import Any


class ExitCode:
    """Process exit codes used by the CLI."""

    SUCCESS = 0
    CONFIGURATION_ERROR = 2
    PROVIDER_ERROR = 3
    WORKER_ERROR = 4
    SANDBOX_ERROR = 5
    COMMAND_FAILURE = 6
    AUTHENTICATION_ERROR = 7
    TIMEOUT = 8
    GENERIC_ERROR = 1


class SandboxPilotError(Exception):
    """Base class for all SandboxPilot errors."""

    code = "sandboxpilot_error"
    http_status = 500
    exit_code = ExitCode.GENERIC_ERROR

    def __init__(
        self,
        message: str,
        *,
        hint: str | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint
        self.details = details or {}
        if cause is not None:
            self.__cause__ = cause

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.hint:
            payload["hint"] = self.hint
        if self.details:
            payload["details"] = self.details
        return payload

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message}\n\n{self.hint}"
        return self.message


class ConfigurationError(SandboxPilotError):
    code = "configuration_error"
    http_status = 400
    exit_code = ExitCode.CONFIGURATION_ERROR


class ProviderError(SandboxPilotError):
    code = "provider_error"
    http_status = 502
    exit_code = ExitCode.PROVIDER_ERROR


class SkyPilotError(ProviderError):
    code = "skypilot_error"


class WorkerProvisionError(ProviderError):
    code = "worker_provision_error"
    exit_code = ExitCode.WORKER_ERROR


class WorkerUnavailableError(SandboxPilotError):
    code = "worker_unavailable"
    http_status = 503
    exit_code = ExitCode.WORKER_ERROR


class WorkerAuthenticationError(SandboxPilotError):
    code = "worker_authentication_error"
    http_status = 401
    exit_code = ExitCode.AUTHENTICATION_ERROR


class AuthenticationError(SandboxPilotError):
    code = "authentication_error"
    http_status = 401
    exit_code = ExitCode.AUTHENTICATION_ERROR


class RuntimeError_(SandboxPilotError):  # noqa: N801 - avoid shadowing builtins.RuntimeError
    code = "runtime_error"
    http_status = 500
    exit_code = ExitCode.SANDBOX_ERROR


SandboxRuntimeError = RuntimeError_


class RuntimeUnavailableError(SandboxRuntimeError):
    code = "runtime_unavailable"
    http_status = 503


class NoCapacityError(SandboxPilotError):
    code = "no_capacity"
    http_status = 503
    exit_code = ExitCode.SANDBOX_ERROR


class CapacityChangedError(NoCapacityError):
    """Worker rejected an admission the control plane believed would fit."""

    code = "capacity_changed"
    http_status = 409


class SandboxNotFoundError(SandboxPilotError):
    code = "sandbox_not_found"
    http_status = 404
    exit_code = ExitCode.SANDBOX_ERROR


class SandboxLostError(SandboxPilotError):
    code = "sandbox_lost"
    http_status = 410
    exit_code = ExitCode.SANDBOX_ERROR


class SandboxNotRunningError(SandboxPilotError):
    code = "sandbox_not_running"
    http_status = 409
    exit_code = ExitCode.SANDBOX_ERROR


class CommandError(SandboxPilotError):
    code = "command_error"
    http_status = 500
    exit_code = ExitCode.COMMAND_FAILURE


class CommandNotFoundError(CommandError):
    code = "command_not_found"
    http_status = 404


class CommandTimeoutError(CommandError):
    code = "command_timeout"
    http_status = 408
    exit_code = ExitCode.TIMEOUT


class FileTransferError(SandboxPilotError):
    code = "file_transfer_error"
    http_status = 400
    exit_code = ExitCode.SANDBOX_ERROR


class NetworkProxyError(SandboxPilotError):
    code = "network_proxy_error"
    http_status = 502
    exit_code = ExitCode.SANDBOX_ERROR


class StateError(SandboxPilotError):
    code = "state_error"
    http_status = 500


class NotFoundError(SandboxPilotError):
    code = "not_found"
    http_status = 404


class ValidationError(SandboxPilotError):
    code = "validation_error"
    http_status = 422
    exit_code = ExitCode.CONFIGURATION_ERROR


class CreateTimeoutError(SandboxPilotError):
    code = "create_timeout"
    http_status = 504
    exit_code = ExitCode.TIMEOUT


class ConflictError(SandboxPilotError):
    code = "conflict"
    http_status = 409


_ERRORS_BY_CODE: dict[str, type[SandboxPilotError]] = {}


def _register_all() -> None:
    stack: list[type[SandboxPilotError]] = [SandboxPilotError]
    while stack:
        cls = stack.pop()
        _ERRORS_BY_CODE.setdefault(cls.code, cls)
        stack.extend(cls.__subclasses__())


_register_all()


def error_from_payload(status: int, payload: Any) -> SandboxPilotError:
    """Reconstruct a typed error from an API error payload."""
    if isinstance(payload, dict):
        body = payload.get("error", payload)
        if isinstance(body, dict):
            code = str(body.get("code", ""))
            cls = _ERRORS_BY_CODE.get(code, SandboxPilotError)
            message = str(body.get("message") or body.get("detail") or f"HTTP {status}")
            return cls(message, hint=body.get("hint"), details=body.get("details"))
        if "detail" in payload:
            cls = AuthenticationError if status == 401 else SandboxPilotError
            return cls(str(payload["detail"]), details={"status": status})
    if status == 401:
        return AuthenticationError("Authentication failed", details={"status": status})
    return SandboxPilotError(f"HTTP {status}: {payload!s}"[:500], details={"status": status})
