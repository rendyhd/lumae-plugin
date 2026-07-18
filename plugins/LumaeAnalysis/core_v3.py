"""AudioMuse 3 registry compatibility adapter.

All v3-only imports are intentionally inside methods. Importing this module is
safe only after ``core_compat.detect_core`` has selected the v3 adapter.
"""


class AudioMuseV3Adapter:
    mode = "v3_registry"

    def list_servers(self):
        import plugin.api as api

        servers = []
        for raw in api.list_servers() or []:
            row = raw if isinstance(raw, dict) else {}
            server_id = row.get("server_id") or row.get("id")
            if not server_id:
                continue
            servers.append(
                {
                    "server_id": str(server_id),
                    "name": str(row.get("name") or "Music server"),
                    "provider_type": str(
                        row.get("provider_type")
                        or row.get("server_type")
                        or row.get("type")
                        or "unknown"
                    ).lower(),
                    "is_default": bool(row.get("is_default")),
                }
            )
        return servers

    def active_server_id(self):
        import plugin.api as api

        value = api.active_server_id()
        return str(value) if value else None

    def bind(self, server_id):
        if not server_id:
            raise ValueError("AudioMuse 3 provider work requires an explicit server_id")
        import plugin.api as api

        return api.use_server(server_id)

    def get_all_songs(self, server_id, apply_filter=True):
        with self.bind(server_id):
            from tasks import mediaserver

            return mediaserver.get_all_songs(apply_filter=apply_filter)

    def list_libraries(self, server_id):
        with self.bind(server_id):
            from tasks import mediaserver

            list_libraries = getattr(mediaserver, "list_libraries", None)
            if not callable(list_libraries):
                return []
            result = list_libraries()
            if isinstance(result, dict):
                return result.get("libraries") or []
            return result or []

    def provider_module(self, provider_type):
        from importlib import import_module

        return import_module(f"tasks.mediaserver.{str(provider_type).lower()}")

    def normalize_analysis_hook(self, song):
        event = dict(song or {})
        server_id = event.get("server_id")
        if not server_id:
            raise ValueError("AudioMuse 3 analysis hook did not include server_id")
        event["server_id"] = str(server_id)
        event["provider_track_id"] = str(event.get("item_id") or "")
        return event

    def analysis_mapping_sql(self):
        return (
            "SELECT provider_track_id, item_id AS analysis_id, match_tier "
            "FROM track_server_map WHERE server_id = %s"
        )
