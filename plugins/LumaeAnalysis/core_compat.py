"""AudioMuse core compatibility detection for the Lumae Analysis plugin.

This module deliberately imports only the plugin API that exists in AudioMuse
2.6. Core versions remain useful diagnostics, but the v3 adapter is admitted
from its observable registry API instead of an exact release list.
"""

from dataclasses import dataclass
import re

from plugin.api import config


SUPPORTED_CORE_MIN = (2, 6, 0)
# Retained as the historically tested range for older clients and diagnostics.
# It is not an admission rule.
SUPPORTED_CORE_RANGE = ">=2.6.0,<4.0.0"
V2_API_CONTRACT = "audiomuse_v2_single_server_v1"
V3_API_CONTRACT = "audiomuse_v3_registry_v1"


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


def _probe_v3_server_api(api_module=None):
    """Read-only proof that the advertised registry has the expected shape."""
    if api_module is None:
        import plugin.api as api_module

    if not _has_v3_server_api(api_module):
        return False
    try:
        servers = api_module.list_servers()
        active = api_module.active_server_id()
    except Exception:
        return False
    if servers is None:
        servers = []
    if not isinstance(servers, (list, tuple)):
        return False
    ids = set()
    for raw in servers:
        if not isinstance(raw, dict):
            return False
        server_id = raw.get("server_id") or raw.get("id")
        if server_id is not None:
            ids.add(str(server_id))
    return active is None or not ids or str(active) in ids


@dataclass(frozen=True)
class CoreCompatibility:
    core_version: str
    parsed_version: tuple | None
    adapter: str | None
    status: str
    supported: bool
    reason: str | None = None
    api_contract: str | None = None

    def as_dict(self):
        return {
            "core_version": self.core_version,
            "core_adapter": self.adapter,
            "core_api_contract": self.api_contract,
            "supported_core_range": SUPPORTED_CORE_RANGE,
            "status": self.status,
            "supported": self.supported,
            "reason": self.reason,
        }


def detect_core(api_module=None, config_obj=None):
    """Select an adapter from observable API capabilities.

    A version change invalidates and reruns this probe, but never admits or
    rejects an otherwise identical API contract by itself.
    """

    cfg = config_obj or config
    raw_version = str(getattr(cfg, "APP_VERSION", "") or "unknown")
    parsed = parse_core_version(raw_version)
    has_v3_api = _has_v3_server_api(api_module)

    if has_v3_api and parsed is not None and parsed[0] < 3:
        return CoreCompatibility(
            raw_version,
            parsed,
            None,
            "core_api_inconsistent",
            False,
            "The reported pre-v3 core unexpectedly exposes the v3 server API.",
        )

    if has_v3_api:
        if not _probe_v3_server_api(api_module):
            return CoreCompatibility(
                raw_version,
                parsed,
                None,
                "core_api_incomplete",
                False,
                "AudioMuse-AI exposes the v3 registry API but its live response is incompatible.",
            )
        return CoreCompatibility(
            raw_version,
            parsed,
            "v3_registry",
            "compatible",
            True,
            api_contract=V3_API_CONTRACT,
        )

    if parsed is None:
        return CoreCompatibility(
            raw_version,
            None,
            None,
            "core_untested",
            False,
            "AudioMuse-AI did not expose a parseable legacy core version or the v3 registry API.",
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

    if parsed[0] == 2:
        return CoreCompatibility(
            raw_version,
            parsed,
            "v2_single_server",
            "compatible",
            True,
            api_contract=V2_API_CONTRACT,
        )

    return CoreCompatibility(
        raw_version,
        parsed,
        None,
        "core_api_incomplete",
        False,
        "AudioMuse-AI is missing the required v3 plugin server-context API.",
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


def get_core_adapter(compatibility=None):
    """Instantiate the selected adapter after compatibility has been proven."""
    selected = compatibility or detect_core()
    if not selected.supported:
        raise RuntimeError(selected.reason or selected.status)
    if selected.adapter == "v2_single_server":
        from .core_v2 import AudioMuseV2Adapter

        return AudioMuseV2Adapter()
    if selected.adapter == "v3_registry":
        from .core_v3 import AudioMuseV3Adapter

        return AudioMuseV3Adapter()
    raise RuntimeError(f"No Lumae core adapter for {selected.adapter!r}")
