"""Explicit single-source projection for the friend catalogue.

Core 3 uses provider-to-analysis identities; the selected server is persisted
and never follows an incidental request's active server. Multiple registries
require an explicit FEDERATED_ALBUMS_SERVER_ID before their first projection.
"""

import os
from contextlib import nullcontext
import plugin.api as api


def source(db, meta_table):
    registry = all(
        callable(getattr(api, name, None))
        for name in ("list_servers", "active_server_id", "use_server")
    )
    with db.cursor() as cur:
        cur.execute(f"SELECT value FROM {meta_table} WHERE key='source_server_id'")
        row = cur.fetchone()
    if not registry:
        version = str(getattr(api.config, "APP_VERSION", "")).lstrip("vV")
        if version.split(".")[0].isdigit() and int(version.split(".")[0]) >= 3:
            raise ValueError("Core 3 friend catalogue requires the server registry API")
        if row and row[0] != "legacy-default":
            raise ValueError("Configured friend catalogue requires the server registry")
        return "legacy-default", False
    ids = [
        str(item.get("server_id") or item.get("id"))
        for item in api.list_servers() or []
    ]
    selected = row[0] if row else os.environ.get("FEDERATED_ALBUMS_SERVER_ID")
    if not selected and len(ids) == 1:
        selected = ids[0]
    if selected not in ids:
        raise ValueError("Select FEDERATED_ALBUMS_SERVER_ID for the friend catalogue")
    with db.cursor() as cur:
        cur.execute(
            f"INSERT INTO {meta_table}(key,value) VALUES('source_server_id',%s) ON CONFLICT(key) DO NOTHING",
            (selected,),
        )
    db.commit()
    return selected, True


def bind(db, meta_table):
    server_id, registry = source(db, meta_table)
    return api.use_server(server_id) if registry else nullcontext()


def mapping(db, meta_table):
    server_id, registry = source(db, meta_table)
    if registry:
        return (
            "JOIN track_server_map m ON m.item_id=s.item_id AND m.server_id=%s",
            [server_id],
            "m.provider_track_id",
        )
    return "", [], "s.item_id"
