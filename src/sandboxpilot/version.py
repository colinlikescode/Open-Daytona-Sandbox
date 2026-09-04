"""Version constants.

Three things are versioned independently:

* ``__version__`` - the Python package version (control plane and worker share it).
* ``API_VERSION`` - the public REST API prefix (``/v1``).
* ``WORKER_PROTOCOL_VERSION`` - the control-plane <-> worker wire protocol.
* ``STATE_SCHEMA_VERSION`` - the SQLite state schema (advanced by migrations).
"""

from __future__ import annotations

__version__ = "0.1.0"

API_VERSION = "v1"
WORKER_PROTOCOL_VERSION = 1
STATE_SCHEMA_VERSION = 1
