"""SkyPilot compute provider. All SkyPilot-specific code lives in this package."""

from sandboxpilot.providers.skypilot.compatibility import SkyPilotCompatibility
from sandboxpilot.providers.skypilot.provider import SkyPilotComputeProvider

__all__ = ["SkyPilotCompatibility", "SkyPilotComputeProvider"]
