"""Credential-contained provider bridge shared by catalogue scanners.

Version-specific dispatcher behavior stays in the core adapters. This module
exposes only sanitized server descriptions and raw provider catalogue objects
to the normalizer; callers must never persist or serialize the bridge itself.
"""

from .core_compat import get_core_adapter


SUPPORTED_PROVIDER_TYPES = frozenset(("navidrome", "jellyfin", "emby", "lyrion"))


class CatalogProviderError(RuntimeError):
    pass


def normalized_provider_type(value):
    return str(value or "").strip().lower()


class ProviderCatalogBridge:
    def __init__(self, core_adapter=None):
        self.core = core_adapter or get_core_adapter()

    def list_servers(self):
        servers = []
        for raw in self.core.list_servers():
            provider_type = normalized_provider_type(raw.get("provider_type"))
            servers.append(
                {
                    "server_id": str(raw["server_id"]),
                    "name": str(raw.get("name") or "Music server"),
                    "provider_type": provider_type,
                    "is_default": bool(raw.get("is_default")),
                    "supported": provider_type in SUPPORTED_PROVIDER_TYPES,
                }
            )
        return servers

    def require_server(self, server_id):
        matches = [server for server in self.list_servers() if server["server_id"] == server_id]
        if not matches:
            raise CatalogProviderError(f"Unknown AudioMuse server: {server_id}")
        server = matches[0]
        if not server["supported"]:
            raise CatalogProviderError(
                f"Provider {server['provider_type'] or 'unknown'} is not supported by Lumae catalogue v2"
            )
        return server

    def list_libraries(self, server_id):
        self.require_server(server_id)
        return self.core.list_libraries(server_id)

    def get_all_songs(self, server_id, apply_filter=True):
        self.require_server(server_id)
        return self.core.get_all_songs(server_id, apply_filter=apply_filter)

    def provider_module(self, server_id):
        server = self.require_server(server_id)
        return self.core.provider_module(server["provider_type"])

