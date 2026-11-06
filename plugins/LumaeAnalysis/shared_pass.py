"""Library passes every worker runner walks at once (multi-worker plan 2026-10-30, R4 and R6).

With ``worker_runners: every`` the leader and the helpers walk a source's edge and loudness
passes together. Each runner takes a *batch* of a few due track IDs at a time, and two batches
never hold the same track:

* ``take`` holds the pass's row (``FOR UPDATE``) while it chooses, so there is one taker at a
  time. It hands out first a batch handed back (``runner_id`` NULL) or one whose runner went
  away (no heartbeat in ``runner_workers`` for WORKER_ONLINE_SECONDS), then the next due tracks
  after the pass's cursor, which it moves past them.
* A runner keeps its batches until its slice ends. ``finish`` then deletes each one, or hands
  back the tracks of it the runner did not finish (not started by the deadline, or cancelled)
  for the next taker.
* A pass is complete only when nothing is due after its cursor and no batch is out. ``take``
  then runs the caller's completion in the same transaction, while it holds the row, so a batch
  handed back meanwhile cannot be stranded behind a finished pass.

Each pass's own per-track rules still apply (an edge job's claim, the due checks): a batch taken
over from a runner that went away may hold tracks that runner finished, and ``refilter`` drops
them before they are handed out again.

The waveform pass selects due tracks rather than walking a cursor, and admits them with
attempt tokens. A runner records each admitted batch with its tokens (``hold``) and forgets it
when its slice ends; ``take_over_gone`` hands any live runner the batches of a runner that went
away, so their attempts are released at once instead of after the hour stale attempts wait.

``acquire_slots`` caps the files analysed at once across every runner (R6). A runner holds one
row of ``analysis_slots`` per analysis thread of its turn and gives them back when the turn ends;
a runner that went away loses them.
"""

from plugin.api import table

from . import migrations, runner


def batches_table():
    return table("pass_batches")


def slots_table():
    return table("analysis_slots")


def migrate(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {batches_table()} (
            batch_id BIGSERIAL PRIMARY KEY,
            pass_name TEXT NOT NULL,
            catalog_instance_id TEXT NOT NULL,
            runner_id TEXT,
            track_ids TEXT[] NOT NULL,
            taken_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            tokens TEXT[]
        )
        """
    )
    # The waveform pass's attempt tokens, parallel to track_ids (``hold``).
    migrations.ensure_columns(cur, batches_table(), "tokens TEXT[]")
    migrations.ensure_index(
        cur,
        f"CREATE INDEX IF NOT EXISTS {batches_table()}_pass_idx "
        f"ON {batches_table()} (pass_name, catalog_instance_id)",
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {slots_table()} (
            slot INTEGER PRIMARY KEY,
            runner_id TEXT NOT NULL,
            taken_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


class Batch:
    """A batch a runner holds: its id, the units handed out (in order) and their track IDs."""

    __slots__ = ("batch_id", "units", "track_ids")

    def __init__(self, batch_id, units, track_ids):
        self.batch_id = batch_id
        self.units = list(units)
        self.track_ids = list(track_ids)


# What ``take`` found when it hands out no batch.
OUT = "out"      # nothing for this runner now: other runners hold the rest, or the pass is parked
ENDED = "ended"  # the pass is complete: ``complete`` ran, now or for another runner
# Statuses of a pass nobody walks until something arms it again.
PARKED = ("waiting_runtime",)


def take(db, pass_name, state_table, catalog_instance_id, size, *, next_units, track_id,
         refilter, complete, runner_id=None):
    """``(Batch, None)``, or ``(None, OUT)`` / ``(None, ENDED)``; one transaction.

    ``next_units(after, size)`` returns the due units after the cursor ``after`` (in cursor
    order), ``track_id(unit)`` a unit's track ID, ``refilter(track_ids)`` the units of those
    tracks that are still due, and ``complete(cur)`` ends the pass. All of them run on ``db``
    inside this transaction.
    """
    runner_id = runner_id or runner.current_runner_id()
    cur = db.cursor()
    try:
        cur.execute(
            f"""SELECT cursor, status, (next_retry_at IS NULL OR next_retry_at <= now())
                  FROM {state_table} WHERE catalog_instance_id=%s FOR UPDATE""",
            (catalog_instance_id,))
        row = cur.fetchone()
        if row is None:
            db.rollback()
            return None, OUT
        cursor, status, due = row[0] or "", row[1], bool(row[2])
        if status == "complete" and not due:
            # Another runner ended this round of the pass: its re-sweep starts the next.
            db.commit()
            return None, ENDED
        if status in PARKED:
            db.commit()
            return None, OUT
        # A batch handed back, or one whose runner went away, first: its tracks are behind the
        # cursor and nobody else would reach them before the pass ends.
        while True:
            cur.execute(
                f"""
                UPDATE {batches_table()} b SET runner_id=%s, taken_at=now()
                 WHERE b.batch_id = (
                       SELECT pb.batch_id FROM {batches_table()} pb
                        WHERE pb.pass_name=%s AND pb.catalog_instance_id=%s
                          AND (pb.runner_id IS NULL OR NOT {runner.live_runner_sql('pb.runner_id')})
                        ORDER BY pb.batch_id LIMIT 1)
                RETURNING b.batch_id, b.track_ids
                """,
                (runner_id, pass_name, catalog_instance_id),
            )
            found = cur.fetchone()
            if found is None:
                break
            units = refilter(list(found[1]))
            if units:
                ids = [track_id(unit) for unit in units]
                cur.execute(f"UPDATE {batches_table()} SET track_ids=%s WHERE batch_id=%s",
                            (ids, found[0]))
                db.commit()
                return Batch(found[0], units, ids), None
            cur.execute(f"DELETE FROM {batches_table()} WHERE batch_id=%s", (found[0],))
        units = next_units(cursor, size)
        if units:
            ids = [track_id(unit) for unit in units]
            cur.execute(
                f"""INSERT INTO {batches_table()} (pass_name, catalog_instance_id, runner_id, track_ids)
                    VALUES (%s, %s, %s, %s) RETURNING batch_id""",
                (pass_name, catalog_instance_id, runner_id, ids),
            )
            batch_id = cur.fetchone()[0]
            cur.execute(
                f"""UPDATE {state_table} SET cursor=%s, status='running',
                       pass_started_at=CASE WHEN COALESCE(cursor, '')='' THEN now()
                                            ELSE pass_started_at END,
                       last_error=NULL, updated_at=now()
                     WHERE catalog_instance_id=%s""",
                (ids[-1], catalog_instance_id),
            )
            db.commit()
            return Batch(batch_id, units, ids), None
        cur.execute(
            f"SELECT 1 FROM {batches_table()} WHERE pass_name=%s AND catalog_instance_id=%s LIMIT 1",
            (pass_name, catalog_instance_id),
        )
        if cur.fetchone() is not None:
            db.commit()
            return None, OUT
        complete(cur)
        db.commit()
        return None, ENDED
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def finish(db, pass_name, state_table, catalog_instance_id, batches, unfinished):
    """End this runner's ``batches`` for its slice: each is deleted, or keeps only the tracks in
    ``unfinished`` (track IDs), handed back for the next taker. Holds the pass's row, as ``take``
    does. Returns the tracks handed back."""
    if not batches:
        return 0
    unfinished = set(unfinished)
    handed_back = 0
    cur = db.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {state_table} WHERE catalog_instance_id=%s FOR UPDATE",
                    (catalog_instance_id,))
        for batch in batches:
            left = [track for track in batch.track_ids if track in unfinished]
            if left:
                cur.execute(
                    f"UPDATE {batches_table()} SET runner_id=NULL, track_ids=%s, taken_at=now() "
                    "WHERE batch_id=%s",
                    (left, batch.batch_id),
                )
                handed_back += len(left)
            else:
                cur.execute(f"DELETE FROM {batches_table()} WHERE batch_id=%s", (batch.batch_id,))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
    return handed_back


def end_slice(db, state_table, catalog_instance_id, processed):
    """Count a runner's slice in the pass's row; a running pass is queued again (another runner,
    or this one's next slice, takes the next batch). A finished or parked pass keeps its status."""
    cur = db.cursor()
    try:
        cur.execute(
            f"""UPDATE {state_table} SET processed=processed+%s,
                   status=CASE WHEN status='running' THEN 'queued' ELSE status END, updated_at=now()
                 WHERE catalog_instance_id=%s""",
            (int(processed or 0), catalog_instance_id),
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def drop(db, pass_name, catalog_instance_id=None):
    """Forget the pass's batches (a pass that starts over, a source removed); returns how many."""
    cur = db.cursor()
    try:
        cur.execute(
            f"DELETE FROM {batches_table()} WHERE pass_name=%s "
            "AND (%s IS NULL OR catalog_instance_id=%s)",
            (pass_name, catalog_instance_id, catalog_instance_id),
        )
        dropped = max(0, cur.rowcount or 0)
    finally:
        cur.close()
    return dropped


def hold(db, pass_name, catalog_instance_id, tokens, runner_id=None):
    """Record a batch the pass admitted on its own: ``tokens`` maps each track ID to its attempt
    token. Returns its ID, for ``forget``."""
    runner_id = runner_id or runner.current_runner_id()
    ids = sorted(tokens)
    cur = db.cursor()
    try:
        cur.execute(
            f"""INSERT INTO {batches_table()} (pass_name, catalog_instance_id, runner_id, track_ids, tokens)
                VALUES (%s, %s, %s, %s, %s) RETURNING batch_id""",
            (pass_name, catalog_instance_id, runner_id, ids, [tokens[track] for track in ids]),
        )
        batch_id = cur.fetchone()[0]
        db.commit()
        return batch_id
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def forget(db, batch_id):
    """The slice that held the batch ended (its attempts finished or were released)."""
    cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {batches_table()} WHERE batch_id=%s", (batch_id,))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def take_over_gone(db, pass_name):
    """Remove the held batches of ``pass_name`` whose runners went away and return them as
    ``[(catalog_instance_id, {track_id: token})]``; each goes to one taker."""
    cur = db.cursor()
    try:
        cur.execute(
            f"""DELETE FROM {batches_table()} pb
                 WHERE pb.pass_name=%s AND pb.tokens IS NOT NULL
                   AND NOT {runner.live_runner_sql('pb.runner_id')}
                RETURNING pb.catalog_instance_id, pb.track_ids, pb.tokens""",
            (pass_name,),
        )
        gone = [(row[0], dict(zip(row[1], row[2]))) for row in cur.fetchall()]
        db.commit()
        return gone
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def batches_out(db, pass_name, catalog_instance_id):
    """Batches of the pass that a runner holds or handed back."""
    cur = db.cursor()
    try:
        cur.execute(
            f"SELECT count(*) FROM {batches_table()} WHERE pass_name=%s AND catalog_instance_id=%s",
            (pass_name, catalog_instance_id),
        )
        return int(cur.fetchone()[0] or 0)
    finally:
        cur.close()


# ------------------------------------------------------------------ slots


def acquire_slots(db, want, cap, runner_id=None):
    """Take up to ``want`` of the ``cap`` analysis slots for this runner; returns how many it
    holds now. Slots of runners that went away are freed first. Never waits: a slot another
    runner takes at the same moment is simply not this one's."""
    runner_id = runner_id or runner.current_runner_id()
    want, cap = max(0, int(want)), max(1, int(cap))
    cur = db.cursor()
    try:
        cur.execute(
            f"DELETE FROM {slots_table()} s WHERE s.slot >= %s OR NOT {runner.live_runner_sql('s.runner_id')}",
            (cap,),
        )
        cur.execute(f"SELECT count(*) FROM {slots_table()} WHERE runner_id=%s", (runner_id,))
        held = int(cur.fetchone()[0] or 0)
        if held < want:
            cur.execute(
                f"""
                INSERT INTO {slots_table()} (slot, runner_id)
                SELECT free.slot, %s FROM generate_series(0, %s - 1) AS free(slot)
                 WHERE NOT EXISTS (SELECT 1 FROM {slots_table()} s WHERE s.slot=free.slot)
                 ORDER BY free.slot LIMIT %s
                ON CONFLICT (slot) DO NOTHING
                """,
                (runner_id, cap, want - held),
            )
            held += max(0, cur.rowcount or 0)
        db.commit()
        return held
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def release_slots(db, runner_id=None):
    """Give back every slot this runner holds; returns how many."""
    runner_id = runner_id or runner.current_runner_id()
    cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {slots_table()} WHERE runner_id=%s", (runner_id,))
        released = max(0, cur.rowcount or 0)
        db.commit()
        return released
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()


def slots_in_use(db):
    """Slots held by runners that are alive."""
    cur = db.cursor()
    try:
        cur.execute(
            f"SELECT count(*) FROM {slots_table()} s WHERE {runner.live_runner_sql('s.runner_id')}"
        )
        return int(cur.fetchone()[0] or 0)
    finally:
        cur.close()
