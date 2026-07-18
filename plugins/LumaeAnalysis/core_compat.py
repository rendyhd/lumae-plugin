"""AudioMuse core compatibility detection for the Lumae Analysis plugin.

This module deliberately imports only the plugin API that exists in AudioMuse
2.6. Optional v3 symbols are inspected lazily so importing the plugin can never
fail before health has a chance to explain an unsupported core.
"""

from dataclasses import dataclass
import re

from plugin.api import config


SUPPORTED_CORE_MIN = (2, 6, 0)
SUPPORTED_CORE_MAX_EXCLUSIVE = (4, 0, 0)
SUPPORTED_CORE_RANGE = ">=2.6.0,<4.0.0"


def parse_core_version(value):
    """Return a numeric three-part core version, or ``None`` when unknown."""
    match = re.search(r"(?:^|[^0-9])(\d+)\.(\d+)(?:\.(\d+))?", str(value or ""))
    if not match:
        return None
    return tuple(int(part or 0) for part in match.groups())


def _has_v3_server_api(api_module=None):
    if api_module is None:
        import plugin.api as api_module

    return all(
        callable(getattr(api_module, name, None))
        for name in ("active_server_id", "list_servers", "use_server")
    )


@dataclass(frozen=True)
class CoreCompatibility:
    core_version: str
    parsed_version: tuple | None
    adapter: str | None
    status: str
    supported: bool
    reason: str | None = None

    def as_dict(self):
        return {
            "core_version": self.core_version,
            "core_adapter": self.adapter,
            "supported_core_range": SUPPORTED_CORE_RANGE,
            "status": self.status,
            "supported": self.supported,
            "reason": self.reason,
        }


def detect_core(api_module=None, config_obj=None):
    """Select the v2 or v3 adapter without importing version-specific code."""
    cfg = config_obj or config
    raw_version = str(getattr(cfg, "APP_VERSION", "") or "unknown")
    parsed = parse_core_version(raw_version)
    has_v3_api = _has_v3_server_api(api_module)

    if parsed is None:
        return CoreCompatibility(
            raw_version,
            None,
            None,
            "core_untested",
            False,
            "AudioMuse-AI did not expose a parseable core version.",
        )

    if parsed < SUPPORTED_CORE_MIN:
        return CoreCompatibility(
            raw_version,
            parsed,
            None,
            "core_too_old",
            False,
            "Lumae Analysis catalogue sync requires AudioMuse-AI 2.6.0 or newer.",
        )

    if parsed >= SUPPORTED_CORE_MAX_EXCLUSIVE:
        return CoreCompatibility(
            raw_version,
            parsed,
            None,
            "core_untested",
            False,
            "This AudioMuse-AI major version has not passed the Lumae compatibility matrix.",
        )

    if parsed[0] == 2:
        if has_v3_api:
            return CoreCompatibility(
                raw_version,
                parsed,
                None,
                "core_api_inconsistent",
                False,
                "The reported v2 core unexpectedly exposes the v3 server API.",
            )
        return CoreCompatibility(raw_version, parsed, "v2_single_server", "compatible", True)

    if parsed[0] == 3:
        if not has_v3_api:
            return CoreCompatibility(
                raw_version,
                parsed,
                None,
                "core_api_incomplete",
                False,
                "AudioMuse-AI v3 is missing its required plugin server-context API.",
            )
        return CoreCompatibility(raw_version, parsed, "v3_registry", "compatible", True)

    return CoreCompatibility(
        raw_version,
        parsed,
        None,
        "core_untested",
        False,
        "This AudioMuse-AI core line is outside the supported compatibility matrix.",
    )


def sanitized_server_summaries(compatibility, api_module=None, config_obj=None):
    """Return credential-free server descriptions for compatibility health."""
    if not compatibility.supported:
        return []

    cfg = config_obj or config
    if compatibility.adapter == "v2_single_server":
        return [
            {
                "server_id": "legacy-default",
                "catalog_instance_id": None,
                "name": "Default music server",
                "provider_type": str(getattr(cfg, "MEDIASERVER_TYPE", "") or "unknown").lower(),
                "is_default": True,
                "status": "not_initialized",
            }
        ]

    if api_module is None:
        import plugin.api as api_module

    summaries = []
    for raw in api_module.list_servers() or []:
        server = raw if isinstance(raw, dict) else {}
        server_id = server.get("server_id") or server.get("id")
        if not server_id:
            continue
        summaries.append(
            {
                "server_id": str(server_id),
                "catalog_instance_id": None,
                "name": str(server.get("name") or "Music server"),
                "provider_type": str(
                    server.get("provider_type")
                    or server.get("server_type")
                    or server.get("type")
                    or "unknown"
                ).lower(),
                "is_default": bool(server.get("is_default")),
                "status": "not_initialized",
            }
        )
    return summaries
