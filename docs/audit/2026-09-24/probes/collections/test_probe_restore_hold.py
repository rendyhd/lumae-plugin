import threading, time
from test_collection_mutations_postgres import collection_api  # noqa: F401


def test_restore_frontier_hold(collection_api, monkeypatch):
    manager, call, connect = collection_api
    marks = {}
    original = manager._record_change

    def rec(*args):
        marks.setdefault("first_event", time.perf_counter())
        original(*args)
    monkeypatch.setattr(manager, "_record_change", rec)
    for n in (500, 20000):
        marks.clear()
        backup = manager._backup_envelope([
            {"name": f"Big{n}", "items": [{"kind": "track", "track_id": f"t{n}-{i}"} for i in range(n)]}], "personal")
        t0 = time.perf_counter()
        r = call("POST", "/api/collections/restore", backup, key=f"big{n}")
        t1 = time.perf_counter()
        print(f"PROBE restore n={n} status={r.status_code} total={t1-t0:.2f}s "
              f"item_phase={marks['first_event']-t0:.2f}s frontier_held={t1-marks['first_event']:.2f}s")
    # 500-item batch upsert (max per request)
    call("POST", "/api/collections", {"id": "b", "name": "b"})
    marks.clear()
    items = [{"kind": "track", "track_id": f"bt{i}"} for i in range(500)]
    t0 = time.perf_counter()
    r = call("POST", "/api/collections/b/items/batch", {"items": items})
    t1 = time.perf_counter()
    print(f"PROBE batch500 status={r.status_code} total={t1-t0:.2f}s frontier_held={t1-marks['first_event']:.2f}s")
