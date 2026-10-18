"""Profile bootstrap create options (K3, K5, K6, SF.3), without a database.

``profile_bootstrap._create_options`` validates a create body before any
query runs, so every rejected value is checked here. One case of each kind
still goes through the route and a database: ``invalid_profile_bootstrap``
and ``invalid_edge_schema`` answer 400 (test_profile_stream_edge_refs_postgres,
test_edge_schema_v3_postgres), and a rejected create writes no session
(test_profile_bootstrap_operability_postgres).
"""

import pytest

from test_lumae_analysis import plugin_api_module  # noqa: F401  (host stub)
from plugins.LumaeAnalysis import profile_bootstrap


def body(**updates):
    return {"protocol_version": 2, "schema_version": 1,
            "transfer_contract": profile_bootstrap.TRANSFER_CONTRACT,
            "catalog_instance_id": "catalog-a", **updates}


def test_defaults():
    assert profile_bootstrap._create_options(body()) == (250, "absolute", None, False, 2)


def test_accepted_values():
    request_id = "6F9619FF-8B86-D011-B42D-00C04FC964FF"
    assert profile_bootstrap._create_options(body(
        page_size=10, expiry_mode="sliding", client_request_id=request_id,
        edge_refs=True, edge_schema=3,
    )) == (10, "sliding", request_id.lower(), True, 3)


@pytest.mark.parametrize("field,value,code", [
    *(("client_request_id", value, "invalid_profile_bootstrap")
      for value in ("not-a-uuid", 7, "a" * 100)),
    *(("expiry_mode", value, "invalid_profile_bootstrap") for value in ("forever", True)),
    *(("edge_refs", value, "invalid_profile_bootstrap")
      for value in ("yes", 1, 0, "true", [], {})),
    *(("edge_schema", value, "invalid_edge_schema")
      for value in (4, 1, "3", 3.0, True, [], {})),
])
def test_invalid_create_options_are_400(field, value, code):
    with pytest.raises(profile_bootstrap.BootstrapError) as exc:
        profile_bootstrap._create_options(body(**{field: value}))
    assert (exc.value.code, exc.value.status) == (code, 400)
