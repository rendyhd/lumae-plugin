"""Schema DDL that takes no lock when there is nothing to do (P2-5).

PostgreSQL takes a DDL statement's table lock before it evaluates
``IF NOT EXISTS``: every ``ALTER TABLE`` (``ADD COLUMN IF NOT EXISTS``,
``DROP DEFAULT``, ``SET NOT NULL`` ...) takes ACCESS EXCLUSIVE, and
``CREATE INDEX IF NOT EXISTS`` takes SHARE, which blocks writers. Re-run on
every install and start, each one queues behind any open reader, and every
later query on the table queues behind it.

The ``ensure_*`` helpers read the catalog first and issue DDL only for what is
missing, so migrating an up-to-date database takes no lock stronger than ROW
EXCLUSIVE. The DDL they do issue is unchanged, so the resulting schema is too.

Needed DDL runs in a savepoint under ``SET LOCAL lock_timeout``
(``DDL_LOCK_TIMEOUT``) and is retried up to ``DDL_LOCK_ATTEMPTS`` times with
backoff (``DDL_RETRY_BACKOFF_S``). A long reader delays an upgrade by seconds
instead of failing the install at the first conflict, and nothing queues
behind a waiting ALTER for longer than the timeout. After the last attempt
the lock-timeout error (SQLSTATE 55P03) propagates, with the transaction
rolled back to before the statement. The caller's ``lock_timeout`` is
restored either way.

The helpers run inside the caller's transaction (migrate is one transaction).
Relations are resolved with ``to_regclass`` on the caller's ``search_path``,
as the DDL itself would be. When a relation does not exist, the DDL runs and
fails as it did before.
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

_SAVEPOINT = "lumae_migration_ddl"
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
                    "lumae_analysis migration gave up waiting for a lock after "
                    "%d attempts of %s: %s",
                    attempt, DDL_LOCK_TIMEOUT, summary,
                )
                raise
            delay = DDL_RETRY_BACKOFF_S[min(attempt, len(DDL_RETRY_BACKOFF_S)) - 1]
            logger.warning(
                "lumae_analysis migration is waiting for a lock (attempt %d/%d, "
                "retry in %.1fs): %s",
                attempt, DDL_LOCK_ATTEMPTS, delay, summary,
            )
            _sleep(delay)
            continue
        cur.execute(f"RELEASE SAVEPOINT {_SAVEPOINT}")
        if previous is not None:
            cur.execute("SELECT set_config('lock_timeout', %s, true)", (previous,))
        return


def lock_table(cur, relation, mode="ACCESS EXCLUSIVE"):
    """``LOCK TABLE`` with the bounded wait and retry of ``run_ddl``."""
    run_ddl(cur, f"LOCK TABLE {relation} IN {mode} MODE")


def column_info(cur, relation, column):
    """``(not_null, default_expression)`` of a column, or None when absent."""
    cur.execute(
        """
        SELECT a.attnotnull, pg_get_expr(d.adbin, d.adrelid)
          FROM pg_attribute a
          LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
         WHERE a.attrelid=to_regclass(%s) AND a.attname=%s
           AND a.attnum > 0 AND NOT a.attisdropped
        """,
        (relation, column),
    )
    return cur.fetchone()


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


def ensure_default(cur, relation, column, expression):
    """``SET DEFAULT expression`` unless it is the current default.

    ``expression`` is compared with ``pg_get_expr`` text (e.g. ``3``); a
    spelling that differs only re-applies the same default.
    """
    info = column_info(cur, relation, _identifier(column))
    if info is not None and info[1] == expression:
        return False
    run_ddl(cur, f"ALTER TABLE {relation} ALTER COLUMN {column} SET DEFAULT {expression}")
    return True


def ensure_no_default(cur, relation, column):
    info = column_info(cur, relation, _identifier(column))
    if info is not None and info[1] is None:
        return False
    run_ddl(cur, f"ALTER TABLE {relation} ALTER COLUMN {column} DROP DEFAULT")
    return True


def ensure_not_null(cur, relation, column):
    info = column_info(cur, relation, _identifier(column))
    if info is not None and info[0]:
        return False
    run_ddl(cur, f"ALTER TABLE {relation} ALTER COLUMN {column} SET NOT NULL")
    return True


def ensure_nullable(cur, relation, column):
    """``DROP NOT NULL`` when the column exists and is NOT NULL; absent is fine."""
    info = column_info(cur, relation, _identifier(column))
    if info is None or not info[0]:
        return False
    run_ddl(cur, f"ALTER TABLE {relation} ALTER COLUMN {column} DROP NOT NULL")
    return True


def ensure_constraint(cur, relation, name, definition):
    """``ADD CONSTRAINT name definition`` unless the table has one by that name."""
    cur.execute(
        "SELECT 1 FROM pg_constraint WHERE conname=%s AND conrelid=to_regclass(%s)",
        (_identifier(name), relation),
    )
    if cur.fetchone() is not None:
        return False
    run_ddl(cur, f"ALTER TABLE {relation} ADD CONSTRAINT {name} {definition}")
    return True


def ensure_index(cur, statement):
    """Run ``CREATE [UNIQUE] INDEX IF NOT EXISTS <name> ON <table> ...`` only
    when no relation of that name exists in the table's schema (where
    PostgreSQL would create it, and what ``IF NOT EXISTS`` checks).

    Only the name is compared, exactly like ``IF NOT EXISTS``: an existing
    index with a different definition is left as it is. To change an index,
    give it a new name, or DROP it and CREATE it explicitly. ``<table>`` must be
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


_EXTENSION_SAVEPOINT = "lumae_migration_extension"


def ensure_extension(cur, name):
    """``CREATE EXTENSION IF NOT EXISTS name`` when it is missing and the role
    may create it. Returns whether the extension is installed afterwards.

    An installed extension (in any schema) issues no DDL. Otherwise the
    statement runs through ``run_ddl`` (bounded lock wait and retries) inside
    its own savepoint: when the role lacks the privilege, the server has no
    such extension, or the lock is still unavailable, the savepoint is rolled
    back, a warning names the SQLSTATE, and migration continues without it.
    The extension goes in the current schema, where the plugin's tables are.
    """
    _identifier(name)
    cur.execute("SELECT 1 FROM pg_extension WHERE extname=%s", (name,))
    if cur.fetchone() is not None:
        return True
    cur.execute(f"SAVEPOINT {_EXTENSION_SAVEPOINT}")
    try:
        run_ddl(cur, f"CREATE EXTENSION IF NOT EXISTS {name}")
    except Exception as exc:
        code = getattr(exc, "pgcode", None)
        if code is None:
            raise
        cur.execute(f"ROLLBACK TO SAVEPOINT {_EXTENSION_SAVEPOINT}")
        cur.execute(f"RELEASE SAVEPOINT {_EXTENSION_SAVEPOINT}")
        logger.warning(
            "lumae_analysis could not create the PostgreSQL extension %s (SQLSTATE %s); "
            "continuing without it. A database owner can run CREATE EXTENSION %s.",
            name, code, name,
        )
        return False
    cur.execute(f"RELEASE SAVEPOINT {_EXTENSION_SAVEPOINT}")
    return True


def apply(cur, steps):
    """Run ``steps`` in order: SQL text is executed, a callable gets ``cur``."""
    for step in steps:
        if callable(step):
            step(cur)
        else:
            cur.execute(step)
