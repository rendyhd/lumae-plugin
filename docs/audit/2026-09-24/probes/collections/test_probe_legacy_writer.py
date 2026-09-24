from test_collection_mutations_postgres import collection_api  # noqa: F401


def test_legacy_default_insert_wedges_frontier(collection_api):
    manager, call, connect = collection_api
    assert call("POST", "/api/collections", {"id": "a", "name": "a"}).status_code == 201
    db = connect()
    with db.cursor() as cur:
        cur.execute("SELECT column_default FROM information_schema.columns WHERE table_name=%s AND column_name='seq'",
                    (manager.collection_changes_table(),))
        print("PROBE seq column default after migration:", cur.fetchone())
        cur.execute(f"SELECT head_seq FROM {manager.collection_feed_state_table()}")
        print("PROBE head before legacy insert:", cur.fetchone()[0])
        # what an un-drained 1.2.5 worker does: INSERT without seq (BIGSERIAL default)
        attempts = []
        for _ in range(5):
            cur.execute("SAVEPOINT s")
            try:
                cur.execute(f"INSERT INTO {manager.collection_changes_table()} (principal, collection_id, entity_kind, entity_id, operation, payload)"
                            " VALUES ('user:alice','a','collection','a','upsert','{}') RETURNING seq")
                attempts.append(("ok", cur.fetchone()[0]))
                break
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT s")
                attempts.append(("fail", type(exc).__name__))
        print("PROBE legacy writer attempts:", attempts)
    db.commit(); db.close()
    results = []
    for i in range(3):
        try:
            r = call("PATCH", "/api/collections/a", {"name": f"n{i}"})
            results.append(r.status_code)
        except Exception as exc:
            results.append(type(exc).__name__ + ":" + str(exc).splitlines()[0][:70])
    print("PROBE subsequent new-writer results:", results)
    other = []
    try:
        other.append(call("POST", "/api/collections", {"id": "z", "name": "z"}, user="bob").status_code)
    except Exception as exc:
        other.append(type(exc).__name__)
    print("PROBE other principal write:", other)
