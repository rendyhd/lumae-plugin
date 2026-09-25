"""Schema DDL that takes no lock when there is nothing to do (P2-8).

A copy of the LumaeAnalysis P2-5 helpers this plugin needs; the plugin is
packaged on its own, so it does not import from LumaeAnalysis.

PostgreSQL takes a DDL statement's table lock before it evaluates
``IF NOT EXISTS``: ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` takes ACCESS
EXCLUSIVE, and ``CREATE INDEX IF NOT EXISTS`` takes SHARE, which blocks
writers. Re-run on every install and web start, each one queues behind any
open reader, and every later query on the table queues behind it.

The ``ensure_*`` helpers read the catalog first and issue DDL only for what is
missing, so migrating an up-to-date database takes no lock stronger than ROW
EXCLUSIVE. The DDL they do issue is unchanged, so the resulting schema is too.

Needed DDL runs in a savepoint under ``SET LOCAL lock_timeout``
(``DDL_LOCK_TIMEOUT``) and is retried up to ``DDL_LOCK_ATTEMPTS`` times with
backoff (``DDL_RETRY_BACKOFF_S``). After the last attempt the lock-timeout
error (SQLSTATE 55P03) propagates, with the transaction rolled back to before
the statement. The caller's ``lock_timeout`` is restored either way.

The helpers run inside the caller's transaction. Relations are resolved with
``to_regclass`` on the caller's ``search_path``, as the DDL itself would be.
"""

import re
import time

from plugin.api import logger


DDL_LOCK_TIMEOUT = "5s"
DDL_LOCK_ATTEMPTS = 3
# Sleep before attempt 2 and attempt 3.
DDL_RETRY_BACKOFF_S = (1.0, 2.0)
LOCK_NOT_AVAILABLE = "55P03"
# PostgreSQL truncates identifiers to NAMEDATALEN - 1 bytes.
MAX_IDENTIFIER_BYTES = 63

_SAVEPOINT = "federated_albums_migration_ddl"
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
# The table name must be unqualified: "ON public.x" would capture "public".
_CREATE_INDEX = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+(\w+)\s+ON\s+(\w+)(?!\w|\s*\.)",
    re.IGNORECASE,
)
# Replaced by tests to observe or shorten the backoff.
_sleep = time.sleep


def _identifier(name):
    if not _IDENTIFIER.match(name):
        raise ValueError(f"not a plain lower-case identifier: {name!r}")
    return name


def run_ddl(cur, statement):
    """Run DDL that needs a table lock, waiting boundedly for it."""
    cur.execute("SELECT current_setting('lock_timeout')")
    row = cur.fetchone()
    previous = row[0] if row else None
    for attempt in range(1, DDL_LOCK_ATTEMPTS + 1):
        cur.execute(f"SAVEPOINT {_SAVEPOINT}")
        # SET LOCAL: undone by ROLLBACK TO SAVEPOINT, restored after success.
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (DDL_LOCK_TIMEOUT,))
        try:
            cur.execute(statement)
        except Exception as exc:
            if getattr(exc, "pgcode", None) != LOCK_NOT_AVAILABLE:
                raise
            cur.execute(f"ROLLBACK TO SAVEPOINT {_SAVEPOINT}")
            cur.execute(f"RELEASE SAVEPOINT {_SAVEPOINT}")
            summary = " ".join(statement.split())[:160]
            if attempt == DDL_LOCK_ATTEMPTS:
                logger.warning(
                    "federated_albums migration gave up waiting for a lock after "
                    "%d attempts of %s: %s",
                    attempt, DDL_LOCK_TIMEOUT, summary,
                )
                raise
            delay = DDL_RETRY_BACKOFF_S[min(attempt, len(DDL_RETRY_BACKOFF_S)) - 1]
            logger.warning(
                "federated_albums migration is waiting for a lock (attempt %d/%d, "
                "retry in %.1fs): %s",
                attempt, DDL_LOCK_ATTEMPTS, delay, summary,
            )
            _sleep(delay)
            continue
        cur.execute(f"RELEASE SAVEPOINT {_SAVEPOINT}")
        if previous is not None:
            cur.execute("SELECT set_config('lock_timeout', %s, true)", (previous,))
        return


def ensure_columns(cur, relation, *definitions):
    """``ADD COLUMN IF NOT EXISTS`` for the definitions whose column is missing.

    Each definition is ``"<name> <type and constraints>"``. The missing ones
    are added in the given order by one ``ALTER TABLE``, so the column order
    matches separate statements. Returns the names that were added.
    """
    names = [_identifier(definition.split(None, 1)[0]) for definition in definitions]
    cur.execute(
        """
        SELECT attname FROM pg_attribute
         WHERE attrelid=to_regclass(%s) AND attnum > 0 AND NOT attisdropped
        """,
        (relation,),
    )
    existing = {row[0] for row in cur.fetchall() or ()}
    missing = [
        (name, definition)
        for name, definition in zip(names, definitions)
        if name not in existing
    ]
    if missing:
        run_ddl(
            cur,
            f"ALTER TABLE {relation} "
            + ", ".join(f"ADD COLUMN IF NOT EXISTS {definition}" for _, definition in missing),
        )
    return [name for name, _ in missing]


def ensure_index(cur, statement):
    """Run ``CREATE [UNIQUE] INDEX IF NOT EXISTS <name> ON <table> ...`` only
    when no relation of that name exists in the table's schema (where
    PostgreSQL would create it, and what ``IF NOT EXISTS`` checks).

    Only the name is compared, exactly like ``IF NOT EXISTS``: an existing
    index with a different definition is left as it is. ``<table>`` must be
    unqualified (it is resolved on the ``search_path``).
    """
    match = _CREATE_INDEX.match(statement)
    if match is None:
        raise ValueError(
            "ensure_index needs CREATE [UNIQUE] INDEX IF NOT EXISTS <name> "
            "ON <unqualified table>"
        )
    name = _identifier(match.group(1).lower())
    relation = match.group(2)
    cur.execute(
        """
        SELECT 1 FROM pg_class
         WHERE relname=%s
           AND relnamespace=(SELECT relnamespace FROM pg_class WHERE oid=to_regclass(%s))
        """,
        (name.encode()[:MAX_IDENTIFIER_BYTES].decode(), relation),
    )
    if cur.fetchone() is not None:
        return False
    run_ddl(cur, statement)
    return True
