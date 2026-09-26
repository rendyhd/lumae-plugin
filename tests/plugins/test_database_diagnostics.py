import pathlib
import sys
import types

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
plugin = types.ModuleType("plugin")
api = types.ModuleType("plugin.api")
api.table = lambda name: f"plugin_lumae_analysis__{name}"
api.config = types.SimpleNamespace(APP_VERSION="v2.6.2", MEDIASERVER_TYPE="navidrome")
api.enqueue = lambda *args, **kwargs: None
api.get_db = lambda: None
api.get_setting = lambda key, default=None: default
api.logger = types.SimpleNamespace(warning=lambda *args, **kwargs: None, exception=lambda *args, **kwargs: None)
api.render_page = lambda body, title=None: body
api.set_setting = lambda *args, **kwargs: None
sys.modules.setdefault("plugin", plugin)
sys.modules.setdefault("plugin.api", api)

from plugins.LumaeAnalysis import database_state as state


class DatabaseError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.pgcode = code


CONTROL = ("SAVEPOINT", "SET LOCAL", "ROLLBACK", "RELEASE", "BEGIN")


class Cursor:
    """Fails the diagnostic query; the savepoint statements succeed."""
    def __init__(self, error=None, row=(7,)):
        self.error, self.row, self.closed = error, row, False
        self.statements = []
    def execute(self, sql, params=None):
        self.statements.append(sql)
        if self.error and not sql.startswith(CONTROL):
            raise self.error
    def fetchone(self):
        return self.row
    def close(self):
        self.closed = True


class Db:
    def __init__(self, cursor):
        self.cursor_value, self.rollbacks = cursor, 0
    def cursor(self):
        return self.cursor_value
    def rollback(self):
        self.rollbacks += 1


def test_safe_error_hides_raw_exception_sql_path_and_token(monkeypatch):
    secret = "postgres://alice:token@host/private.sql"
    cursor = Cursor(DatabaseError(f"SELECT * FROM secret {secret}", "42P01"))
    errors, diagnostics = [], []
    clocks = iter((10.0, 10.025))
    monkeypatch.setattr(state, "monotonic", lambda: next(clocks))
    db = Db(cursor)
    # A failed read is unavailable, never the zero default.
    assert state._fetchone(db, "SELECT secret", (secret,), errors, diagnostics, "sonic links", (0,)) is state.UNAVAILABLE
    assert cursor.closed
    assert errors == [{"section": "sonic links", "operation": "sonic_links_summary", "message": "Database diagnostic query failed.", "error_class": "database_error", "sqlstate": "42P01"}]
    assert diagnostics == [{"operation": "sonic_links_summary", "server_db_execute_fetch_ms": 25, "status": "error", "error_class": "database_error", "sqlstate": "42P01"}]
    assert secret not in repr(errors) + repr(diagnostics)
    # The failed read is undone to its savepoint; the host transaction stays.
    assert cursor.statements == [
        "SAVEPOINT lumae_diagnostic_read",
        f"SET LOCAL statement_timeout = {state.DEFAULT_DIAGNOSTIC_STATEMENT_TIMEOUT_MS}",
        "SELECT secret",
        "ROLLBACK TO SAVEPOINT lumae_diagnostic_read",
        "RELEASE SAVEPOINT lumae_diagnostic_read",
    ]
    assert db.rollbacks == 0


def test_timing_success_and_malformed_sqlstate_are_bounded(monkeypatch):
    cursor = Cursor(row=(9,))
    clocks = iter((1.0, 1.011))
    monkeypatch.setattr(state, "monotonic", lambda: next(clocks))
    errors, diagnostics = [], []
    assert state._fetchone(Db(cursor), "SELECT 1", (), errors, diagnostics, "analysis items", (0,)) == (9,)
    assert diagnostics == [{"operation": "analysis_items_summary", "server_db_execute_fetch_ms": 11, "status": "ok"}]
    assert state._safe_sqlstate(DatabaseError("bad", "not-a-code")) is None
    # A successful read is rolled back to its savepoint too: SET LOCAL ends there.
    assert cursor.statements[:2] == [
        "SAVEPOINT lumae_diagnostic_read",
        f"SET LOCAL statement_timeout = {state.DEFAULT_DIAGNOSTIC_STATEMENT_TIMEOUT_MS}",
    ]
    # Two statements: nothing relies on several statements per execute.
    assert cursor.statements[-2:] == [
        "ROLLBACK TO SAVEPOINT lumae_diagnostic_read",
        "RELEASE SAVEPOINT lumae_diagnostic_read",
    ]


def test_an_autocommit_connection_reads_in_a_transaction_it_owns_and_rolls_back():
    cursor = Cursor(row=(3,))
    db = Db(cursor)
    db.autocommit = True
    errors, diagnostics = [], []
    assert state._fetchone(db, "SELECT 1", (), errors, diagnostics, "analysis items", (0,)) == (3,)
    assert cursor.statements == [
        "BEGIN",
        f"SET LOCAL statement_timeout = {state.DEFAULT_DIAGNOSTIC_STATEMENT_TIMEOUT_MS}",
        "SELECT 1",
        "ROLLBACK",
    ]
    assert errors == []


def test_cursor_creation_failure_and_record_bound_are_safe():
    class BadDb:
        def cursor(self):
            raise DatabaseError("/private/token", "08006")
        def rollback(self):
            raise AssertionError("must not run")
    errors, diagnostics = [], []
    assert state._fetchall(BadDb(), "SELECT x", (), errors, diagnostics, "analysis run workflow") is state.UNAVAILABLE
    assert errors[0]["error_class"] == "connection"
    assert diagnostics == []
    for _ in range(30):
        state._record_diagnostic(diagnostics, "sonic links", 0)
    assert len(diagnostics) == state._MAX_DIAGNOSTIC_OPERATIONS

def test_rollback_and_close_failures_do_not_expose_raw_errors():
    class BrokenCursor(Cursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if sql.startswith("ROLLBACK TO"):
                raise DatabaseError("savepoint-private-token", "08006")
        def close(self):
            raise DatabaseError("close-private-token", "08006")

    class BrokenDb(Db):
        def rollback(self):
            raise DatabaseError("rollback-private-token", "08006")

    errors, diagnostics = [], []
    db = BrokenDb(BrokenCursor(DatabaseError("query-private-token", "42P01")))
    assert state._fetchone(db, "SELECT secret", (), errors, diagnostics, "sonic links", (0,)) is state.UNAVAILABLE
    # The query; the transaction rollback tried after the savepoint rollback
    # failed; close.
    assert [row["sqlstate"] for row in errors] == ["42P01", "08006", "08006"]
    assert diagnostics[0]["status"] == "error"
    assert "private-token" not in repr(errors) + repr(diagnostics)


def test_the_timeout_setting_is_clamped_and_falls_back_to_the_default(monkeypatch):
    def setting(value):
        monkeypatch.setattr(state, "get_setting", lambda key, default=None: value)
        return state.diagnostic_timeout_ms()

    assert state.DEFAULT_DIAGNOSTIC_STATEMENT_TIMEOUT_MS == 5000
    assert setting("12000") == 12000
    assert setting(50) == state.MIN_DIAGNOSTIC_STATEMENT_TIMEOUT_MS == 1000
    assert setting(999999) == state.MAX_DIAGNOSTIC_STATEMENT_TIMEOUT_MS == 30000
    assert setting("not a number") == 5000
    assert setting(None) == 5000

    def broken(key, default=None):
        raise DatabaseError("settings table unavailable", "08006")

    monkeypatch.setattr(state, "get_setting", broken)
    assert state.diagnostic_timeout_ms() == 5000
    cursor = Cursor(row=(4,))
    assert state._fetchone(Db(cursor), "SELECT 1", (), [], [], "analysis items", (0,)) == (4,)
    assert cursor.statements[1] == "SET LOCAL statement_timeout = 5000"
