"""Sandbox runtimes.

* :class:`GVisorDockerRuntime` - production: Docker Engine API with ``runtime=runsc``.
* :class:`FakeSandboxRuntime` - tests: in-memory simulation, no Docker required.
* ``docker-unsafe`` - development only: Docker with the default runtime. Requires
  ``SANDBOXPILOT_DEV_UNSAFE_RUNTIME=1`` and is never selected implicitly.
"""

from sandboxpilot.worker.runtime.base import (
    ExecHandle,
    ImageInfo,
    RuntimeDoctorResult,
    SandboxRuntime,
)

__all__ = ["ExecHandle", "ImageInfo", "RuntimeDoctorResult", "SandboxRuntime"]
