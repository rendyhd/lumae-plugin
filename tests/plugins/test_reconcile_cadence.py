"""The watchdog's cadence around AudioMuse's own schedules (plan S1, AUD-13).

* While it yields, the watchdog ticks once at a random minute 5-15 minutes
  ahead: AudioMuse's retry pass runs about every 11 minutes, and a fixed
  cadence could meet it on every pass.
* Minute cadence leaves out the minutes at which Lumae's other enabled rows
  are due: AudioMuse's minute pass dispatches due rows in cron.id order, and
  the watchdog's row comes first, so a watchdog due in the same minute was
  queued first and the other row refused.
"""
import random
import re
import time

import pytest

from test_lumae_analysis import load_plugin  # noqa: F401  (installs the host stubs)
from plugins.LumaeAnalysis import reconcile  # noqa: E402


def _host_field_matches(field, value):
    """AudioMuse's app_cron._field_matches for the minute field (0-59)."""
    if field.strip() == "*":
        return True
    for part in field.split(","):
        part = part.strip()
        try:
            if "/" in part:
                base, step = part.split("/", 1)
                step = int(step)
                if step <= 0:
                    continue
                if base.strip() == "*":
                    if value % step == 0:
                        return True
                elif "-" in base:
                    low, high = (int(item) for item in base.split("-", 1))
                    if low <= value <= high and (value - low) % step == 0:
                        return True
                elif value >= int(base) and (value - int(base)) % step == 0:
                    return True
            elif "-" in part:
                low, high = (int(item) for item in part.split("-", 1))
                if low <= value <= high:
                    return True
            elif int(part) == value:
                return True
        except ValueError:
            continue
    return False


@pytest.mark.parametrize("expr", [
    "* * * * *", "*/5 * * * *", "*/15 * * * *", "11 * * * *", "2,32 * * * *",
    "23 3 * * *", "17 */6 * * *", "0,1,3-31,33-59 * * * *", "10-40/7 * * * *", "5/20 * * * *",
    "1,2,3,10-12 * * * *",
])
def test_minutes_are_read_as_audiomuse_reads_them(expr):
    assert reconcile.cron_minutes(expr) == {
        minute for minute in range(60) if _host_field_matches(expr.split()[0], minute)}


@pytest.mark.parametrize("expr", ["", "x * * * *", "61 * * * *", "5-2 * * * *", "*/0 * * * *"])
def test_an_unparseable_minute_field_is_none(expr):
    assert reconcile.cron_minutes(expr) is None


def test_minute_cadence_skips_the_minutes_of_lumaes_other_rows():
    avoid = reconcile.avoid_minutes
    assert avoid("* * * * *", {2, 32}) == "0,1,3-31,33-59 * * * *"
    assert avoid("* * * * *", {2, 23, 32}) == "0,1,3-22,24-31,33-59 * * * *"
    assert avoid("* * * * *", {2, 17, 23, 32, 47}) == "0,1,3-16,18-22,24-31,33-46,48-59 * * * *"
    assert avoid("* * * * *", set()) == "* * * * *"
    # The other cadences keep their minutes unless one is taken.
    assert avoid("*/5 * * * *", {2, 23, 32}) == "*/5 * * * *"
    assert avoid("*/5 * * * *", {5}) == "0,10,15,20,25,30,35,40,45,50,55 * * * *"
    assert avoid("11 * * * *", {2, 32}) == "11 * * * *"
    # Never no minute at all.
    assert avoid("11 * * * *", {11}) == "11 * * * *"
    for minutes in ({2, 32}, {0, 59}, {1, 2, 3}):
        field = avoid("* * * * *", minutes).split()[0]
        assert {m for m in range(60) if _host_field_matches(field, m)} == set(range(60)) - minutes


def test_a_thousand_yield_picks_are_one_minute_five_to_fifteen_minutes_ahead():
    rng = random.Random(20261011)
    for _ in range(1000):
        now = rng.uniform(0, 2 * 10 ** 9)
        expr = reconcile.yield_cron({2, 32}, now=now, rng=rng)
        assert re.fullmatch(r"\d{1,2} \* \* \* \*", expr), expr
        ahead = (int(expr.split()[0]) - time.localtime(now).tm_min) % 60
        assert 5 <= ahead <= 15
        assert int(expr.split()[0]) not in {2, 32}


def test_a_yield_pick_takes_a_lumae_minute_only_when_nothing_else_is_ahead():
    now = time.mktime((2026, 10, 11, 2, 0, 0, 0, 0, -1))
    excluded = set(range(60)) - {9}
    assert reconcile.yield_cron(excluded, now=now) == "9 * * * *"
    assert reconcile.yield_cron(set(range(60)), now=now).split()[0] in {
        str(minute) for minute in range(5, 16)}


def _simulate_a_day(rows):
    """AudioMuse's minute pass for 24 hours, rows in cron.id order (the
    watchdog first); a plugin task is live for its run time. Returns the
    refusals per task type."""
    live_until = 0
    refused = {}
    for minute_of_day in range(24 * 60):
        hour, minute = divmod(minute_of_day, 60)
        now = minute_of_day * 60
        for task_type, expr, seconds in rows:
            fields = expr.split()
            if not _host_field_matches(fields[0], minute):
                continue
            if fields[1] != "*" and not _host_field_matches(fields[1], hour):
                continue
            if now < live_until:
                refused[task_type] = refused.get(task_type, 0) + 1
                continue
            live_until = now + seconds
    return refused


@pytest.mark.parametrize("tick_seconds", [1, 30, 50, 59])
def test_a_day_of_minute_ticks_never_refuses_lumaes_other_rows(tick_seconds):
    others = [("provider_identity_recheck", "2,32 * * * *", 5),
              ("collection_retention", "23 3 * * *", 20)]
    excluded = set()
    for _task, expr, _seconds in others:
        excluded |= reconcile.cron_minutes(expr)
    watchdog = reconcile.avoid_minutes(reconcile.ACTIVE_CRON, excluded)
    assert _simulate_a_day([("watchdog", watchdog, tick_seconds)] + others) == {}
    # 1.6.1: the watchdog at "* * * * *" was queued first at :02 and :32.
    before = _simulate_a_day([("watchdog", reconcile.ACTIVE_CRON, tick_seconds)] + others)
    assert before == {"provider_identity_recheck": 48, "collection_retention": 1}
