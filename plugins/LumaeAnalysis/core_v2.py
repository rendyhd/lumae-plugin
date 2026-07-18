"""AudioMuse 2.6 single-server compatibility adapter."""

from contextlib import nullcontext

from plugin.api import config


class AudioMuseV2Adapter:
    mode = "v2_single_server"

    def list_servers(self):
        return [
            {
                "server_id": "legacy-default",
                "name": "Default music server",
                "provider_type": str(getattr(config, "MEDIASERVER_TYPE", "") or "unknown").lower(),
                "is_default": True,
            }
        ]

    def bind(self, server_id):
        if server_id not in (None, "legacy-default"):
            raise KeyError(f"Unknown AudioMuse 2.6 server: {server_id}")
        return nullcontext()

    def get_all_songs(self, server_id="legacy-default", apply_filter=True):
        with self.bind(server_id):
            from tasks import mediaserver

            try:
                return mediaserver.get_all_songs(apply_filter=apply_filter)
            except TypeError as exc:
                # AudioMuse 2.6.0 did not consistently expose apply_filter on
                # every dispatcher/provider combination.
                if "apply_filter" not in str(exc):
                    raise
                return mediaserver.get_all_songs()

    def list_libraries(self, server_id="legacy-default"):
        with self.bind(server_id):
            from tasks import mediaserver

            list_libraries = getattr(mediaserver, "list_libraries", None)
            if not callable(list_libraries):
                return []
            result = list_libraries()
            if isinstance(result, dict):
                return result.get("libraries") or []
            return result or []

    def provider_module(self, provider_type=None):
        from importlib import import_module

        name = str(provider_type or getattr(config, "MEDIASERVER_TYPE", "") or "").lower()
        if not name:
            raise RuntimeError("AudioMuse 2.6 has no configured media-server type")
        return import_module(f"tasks.mediaserver.{name}")

    def normalize_analysis_hook(self, song):
        event = dict(song or {})
        event["server_id"] = "legacy-default"
        event["server_name"] = "Default music server"
        event["provider_track_id"] = str(event.get("item_id") or "")
        return event

    def analysis_mapping_sql(self):
        return (
            "SELECT item_id AS provider_track_id, item_id AS analysis_id, "
            "'direct'::text AS match_tier FROM score"
        )
