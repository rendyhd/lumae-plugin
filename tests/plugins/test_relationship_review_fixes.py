"""Relationship build review fixes that need no database."""

import time

import numpy as np
import pytest

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import catalog_enrichment as e, relationship_build as rb


class _Cursor:
    """Answers the input_state query with ``row``."""

    def __init__(self, row):
        self.row = row

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return self.row


UNPUBLISHED = ('server-a', 'active', 1, 'catalog-epoch', 'running', 1, 'analysis-epoch', 'complete')


def test_unpublished_inputs_are_their_own_kind_of_input_change():
    with pytest.raises(rb.InputsNotPublished):
        rb.input_state(_Cursor(UNPUBLISHED), 'catalog-a')
    with pytest.raises(rb.InputsNotPublished):
        rb.input_state(_Cursor(None), 'catalog-a')


def test_inputs_unpublished_under_a_running_build_restart_it_as_before():
    build = rb.Build(None, 'catalog-a', None, 8, 10)
    build.identity, build.inputs_staged = {'catalog_generation': 1}, True
    with pytest.raises(rb.InputsChanged) as raised:
        build.verify_inputs(_Cursor(UNPUBLISHED))
    assert not isinstance(raised.value, rb.InputsNotPublished)


def test_the_budget_clock_starts_after_loading_not_at_construction():
    build = rb.Build(None, 'catalog-a', None, 8, 0.01)
    time.sleep(0.03)  # a slow checkpoint load
    assert not build.exhausted()
    build.start_clock()
    assert not build.exhausted()
    first = build.deadline
    build.start_clock()  # once per step
    assert build.deadline == first
    time.sleep(0.03)
    assert build.exhausted()


def test_batch_size_still_bounds_a_step_before_the_clock_starts():
    build = rb.Build(None, 'catalog-a', None, 2, 10)
    build.work = 2
    assert build.exhausted()


def test_packed_entities_unpack_only_what_is_read():
    entity = {'key': 'a', 'fingerprint': {'mean': np.arange(4, dtype='<f4')}}
    blob = rb.pack_entity(entity)
    entities = rb.PackedEntities()
    entities['a'] = memoryview(blob)
    entities['b'] = b'not a valid archive'  # never read
    assert len(entities) == 2
    first = entities['a']
    assert first['key'] == 'a'
    np.testing.assert_array_equal(first['fingerprint']['mean'], entity['fingerprint']['mean'])
    assert entities['a'] is first  # unpacked once


def test_the_ivf_lookup_can_load_its_index_ahead_of_the_budget():
    assert e._ivf_candidate_analysis_ids.prepare is e._ivf_index


def test_scoring_loads_the_candidate_index_before_its_clock(monkeypatch):
    events = []

    def lookup(*_):
        return []
    lookup.prepare = lambda: events.append(('prepare', build.deadline))

    class Cur:
        def __init__(self):
            self.rows = []

        def execute(self, sql, params=None):
            if 'relationship_build_entities' in sql and 'SELECT' in sql:
                self.rows = [('artist', 'a', rb.pack_entity({
                    'key': 'a', 'artist': 'A', 'cover': None,
                    'fingerprint': {'core': [np.ones(4, dtype='<f4')]}}), None)]
            else:
                self.rows = []

        def fetchall(self):
            return self.rows

    class Db:
        def commit(self):
            pass

    build = rb.Build(Db(), 'catalog-a', None, 8, 10)
    build.build_id, build.phase = 'b', 'scoring'
    monkeypatch.setattr(rb.Build, 'checkpoint', lambda self, cur, phase=None: events.append(('cp', phase)))
    monkeypatch.setattr(e, '_rank_artists', lambda entity, candidates: [])
    build.score(Cur(), lookup)
    assert events[0] == ('prepare', None)
    assert build.deadline is not None and build.work == 1
