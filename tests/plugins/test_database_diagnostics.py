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


class Cursor:
    def __init__(self, error=None, row=(7,)):
        self.error, self.row, self.closed = error, row, False
    def execute(self, sql, params):
        if self.error:
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
    assert state._fetchone(Db(cursor), "SELECT secret", (secret,), errors, diagnostics, "sonic links", (0,)) == (0,)
    assert cursor.closed
    assert errors == [{"section": "sonic links", "operation": "sonic_links_summary", "message": "Database diagnostic query failed.", "error_class": "database_error", "sqlstate": "42P01"}]
    assert diagnostics == [{"operation": "sonic_links_summary", "server_db_execute_fetch_ms": 25, "status": "error", "error_class": "database_error", "sqlstate": "42P01"}]
    assert secret not in repr(errors) + repr(diagnostics)


def test_timing_success_and_malformed_sqlstate_are_bounded(monkeypatch):
    cursor = Cursor(row=(9,))
    clocks = iter((1.0, 1.011))
    monkeypatch.setattr(state, "monotonic", lambda: next(clocks))
    errors, diagnostics = [], []
    assert state._fetchone(Db(cursor), "SELECT 1", (), errors, diagnostics, "analysis items", (0,)) == (9,)
    assert diagnostics == [{"operation": "analysis_items_summary", "server_db_execute_fetch_ms": 11, "status": "ok"}]
    assert state._safe_sqlstate(DatabaseError("bad", "not-a-code")) is None


def test_cursor_creation_failure_and_record_bound_are_safe():
    class BadDb:
        def cursor(self):
            raise DatabaseError("/private/token", "08006")
        def rollback(self):
            raise AssertionError("must not run")
    errors, diagnostics = [], []
    assert state._fetchall(BadDb(), "SELECT x", (), errors, diagnostics, "analysis run workflow") == []
    assert errors[0]["error_class"] == "connection"
    assert diagnostics == []
    for _ in range(30):
        state._record_diagnostic(diagnostics, "sonic links", 0)
    assert len(diagnostics) == state._MAX_DIAGNOSTIC_OPERATIONS

def test_rollback_and_close_failures_do_not_expose_raw_errors():
    class BrokenCursor(Cursor):
        def close(self):
            raise DatabaseError("close-private-token", "08006")

    class BrokenDb(Db):
        def rollback(self):
            raise DatabaseError("rollback-private-token", "08006")

    errors, diagnostics = [], []
    db = BrokenDb(BrokenCursor(DatabaseError("query-private-token", "42P01")))
    assert state._fetchone(db, "SELECT secret", (), errors, diagnostics, "sonic links", (0,)) == (0,)
    assert [row["sqlstate"] for row in errors] == ["42P01", "08006", "08006"]
    assert diagnostics[0]["status"] == "error"
    assert "private-token" not in repr(errors) + repr(diagnostics)
