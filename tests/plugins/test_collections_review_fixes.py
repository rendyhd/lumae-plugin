"""Review fixes for the per-user sync features that need no database.

Workbench batch chunking (run in node when it is installed), escaping in the
Undertone page, the streaming proxies releasing the request context, the
Navidrome artwork cover id, malformed input, and the MusicBrainz body cap.
"""
import importlib
import json
import math
import re
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from flask import Blueprint, Flask

from test_lumae_analysis import load_plugin


def _module(name):
    load_plugin()
    return importlib.import_module(f"plugins.LumaeAnalysis.{name}")


# ------------------------------------------------------------------ 1. workbench batches


def _workbench_functions():
    ui = _module("collection_ui")
    page = ui.render_collection_workbench("Label", "Detail")
    names = ("batchChunks", "duplicateCollection", "moveItem", "removeSelected")
    found = {}
    for line in page.splitlines():
        for name in names:
            if line.startswith(f"async function {name}("):
                found[name] = line
    assert set(found) == set(names)
    assert "const BATCH_LIMIT=500;" in page
    return "\n".join(found[name] for name in names)


HARNESS = r"""
const BATCH_LIMIT=500,api='/api/collections';let calls=[],failAt=null,revision=1,keys=0,toasts=[],loads=0,refreshes=[];
const mutationKey=()=>`k${++keys}`,toast=m=>toasts.push(m),confirm=()=>true;
async function loadCollection(){loads+=1}
async function refreshCollections(id){refreshes.push(id)}
async function req(url,opts={}){const body=opts.body?JSON.parse(opts.body):null;calls.push({url,method:opts.method||'GET',ifMatch:opts.headers?.['If-Match'],body});if(failAt!==null&&calls.length===failAt)throw Object.assign(new Error('boom'),{status:503});if(url===api&&opts.method==='POST')return{collection:{id:'copy',name:'Mix copy',revision:1}};if(opts.method==='DELETE'&&!url.endsWith('/batch'))return{};revision+=1;return{collection:{revision},deleted_count:body?.item_ids?.length||0}}
let current={id:'mix',name:'Mix',description:null,revision:1},items=[],checkedItems=new Set();
__FUNCTIONS__
(async()=>{const out={};
items=Array.from({length:1201},(_,i)=>({id:`i${i}`,collection_id:'mix',kind:'track',track_id:`t${i}`,position:i}));
await duplicateCollection();out.duplicate=calls.map(c=>[c.url,c.method,c.ifMatch,c.body?.items?.length??null]);
calls=[];revision=1;current.revision=7;revision=7;await moveItem('i10',1);out.move=calls.map(c=>[c.ifMatch,c.body.items.map(i=>[i.id,i.position])]);
calls=[];current.revision=7;revision=7;checkedItems=new Set(items.map(i=>i.id));await removeSelected();out.remove=calls.map(c=>[c.ifMatch,c.body.item_ids.length]);out.toasts=toasts.slice();
calls=[];refreshes=[];revision=1;failAt=3;try{await duplicateCollection()}catch(e){out.failed=e.message}out.failedCalls=calls.map(c=>[c.url,c.method,c.ifMatch]);
console.log(JSON.stringify(out))})().catch(e=>{console.error(e);process.exit(1)});
"""


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_workbench_batches_are_chunked_at_500_and_chained_on_revisions(tmp_path):
    script = tmp_path / "harness.js"
    script.write_text(HARNESS.replace("__FUNCTIONS__", _workbench_functions()), encoding="utf-8")
    out = json.loads(subprocess.run(["node", str(script)], check=True, capture_output=True,
                                    text=True, encoding="utf-8").stdout)
    # Create, then 500 + 500 + 201 items, each If-Match the revision the last answer gave.
    assert out["duplicate"] == [
        ["/api/collections", "POST", None, None],
        ["/api/collections/copy/items/batch", "POST", "1", 500],
        ["/api/collections/copy/items/batch", "POST", "2", 500],
        ["/api/collections/copy/items/batch", "POST", "3", 201],
    ]
    # A swap sends only the two rows whose position changes.
    assert out["move"] == [["7", [["i11", 10], ["i10", 11]]]]
    assert out["remove"] == [["7", 500], ["8", 500], ["9", 201]]
    assert out["toasts"][-1] == "Removed 1201 items."
    # A failed later chunk removes the incomplete copy (If-Match its latest revision).
    assert out["failedCalls"][-1] == ["/api/collections/copy", "DELETE", "2"]
    assert "incomplete copy was removed" in out["failed"]


# ------------------------------------------------------------------ 2. escaping


def test_undertone_preset_editor_escapes_numeric_fields():
    ui = _module("undertone_ui")
    source = open(ui.__file__, encoding="utf-8").read()
    edit = next(line for line in source.splitlines() if line.startswith("function editPreset("))
    assert "value=\"${esc(t?.kind==='minutes'?t.minutes:'')}\"" in edit
    assert 'value="${esc(p.fadeInSeconds??0)}"' in edit
    # Every attribute value in the page is escaped or a computed number.
    for expression in re.findall(r'value="\$\{(.*?)\}"', source):
        assert expression.startswith(("esc(", "Math.round(")) or expression in ("h", "k"), expression


def test_workbench_escapes_catalogue_numbers_and_years():
    ui = _module("collection_ui")
    page = ui.render_collection_workbench("Label", "Detail")
    for raw in ("${item.year}", "${album.year}", "${item.track_count||0}", "Disc ${disc}",
                "${number??''}", "<span>${c.album_count} albums"):
        assert raw not in page, raw


# ------------------------------------------------------------------ 3 and 9. proxies


class _Upstream:
    def __init__(self, events):
        self.events = events
        self.status_code = 200
        self.headers = {"Content-Type": "image/jpeg"}
        self.closed = False

    def iter_content(self, chunk_size):
        self.events.append("chunk")
        yield b"data"

    def close(self):
        self.closed = True


@pytest.mark.parametrize("path", ["/api/collections/library/art/t1", "/api/collections/library/stream/t1"])
def test_streaming_proxies_end_the_read_transaction_and_keep_no_request_context(monkeypatch, path):
    library = _module("collection_library")
    events = []
    upstream = _Upstream(events)
    monkeypatch.setattr(library, "_route_catalog", lambda: ("catalog-a", "navidrome"))
    monkeypatch.setattr(library, "_with_source_module", lambda catalog, provider, build: build(None))
    monkeypatch.setattr(library, "_resolve_art_target", lambda *a, **k: ("http://nd/art", {}, {}))
    monkeypatch.setattr(library, "_resolve_stream_target", lambda *a, **k: (("http://nd/s", {}, {}), None))
    monkeypatch.setattr(library.http_requests, "get", lambda *a, **k: upstream)
    monkeypatch.setattr(library, "get_db", lambda: SimpleNamespace(commit=lambda: events.append("commit")))
    app = Flask(__name__)
    blueprint = Blueprint("library_stream", __name__)
    library.register_collection_library_routes(blueprint, lambda view: view)
    app.register_blueprint(blueprint)
    app.teardown_request(lambda _exc: events.append("teardown"))
    response = app.test_client().get(path, buffered=False)
    assert response.status_code == 200
    assert b"".join(response.response) == b"data"
    response.close()
    # The transaction ends and the request context is torn down before the
    # first byte is streamed: the connection is not held for the stream.
    assert events == ["commit", "teardown", "chunk"]
    assert upstream.closed


def _navidrome(monkeypatch, library, mirror):
    songs = []
    bound = SimpleNamespace(_navidrome_request=lambda name, params: songs.append(params["id"])
                            or {"song": {"coverArt": "al-from-song"}})
    monkeypatch.setattr(library, "_navidrome_access", lambda module: ("http://nd", {"u": "x"}, bound))
    monkeypatch.setattr(library, "_mirror_cover_art_id", lambda catalog, track: mirror)
    return songs


def test_navidrome_artwork_reads_the_cover_id_from_the_mirror(monkeypatch):
    library = _module("collection_library")
    songs = _navidrome(monkeypatch, library, "al-from-mirror")
    url, _headers, params = library._resolve_art_target("t1", 320, "navidrome", catalog_instance_id="c")
    assert url == "http://nd/rest/getCoverArt.view"
    assert params["id"] == "al-from-mirror" and songs == []


def test_navidrome_artwork_falls_back_to_get_song_without_a_mirrored_cover(monkeypatch):
    library = _module("collection_library")
    songs = _navidrome(monkeypatch, library, None)
    _url, _headers, params = library._resolve_art_target("t1", 320, "navidrome", catalog_instance_id="c")
    assert params["id"] == "al-from-song" and songs == ["t1"]


# ------------------------------------------------------------------ 10. malformed input


@pytest.mark.parametrize("raw", [
    "not an object", [1], {"kind": "track", "track_id": "t", "position": math.inf},
    {"kind": "track", "track_id": "t", "position": 2**31},
])
def test_malformed_items_are_value_errors(raw):
    manager = _module("collection_manager")
    with pytest.raises(ValueError):
        manager._normalize_item(raw)


def test_largest_position_is_accepted():
    manager = _module("collection_manager")
    item = manager._normalize_item({"kind": "track", "track_id": "t", "position": 2**31 - 1})
    assert item["position"] == 2**31 - 1
    assert manager._normalize_item({"kind": "track", "track_id": "t", "position": -3})["position"] == 0


def test_non_finite_numbers_are_refused_by_every_mutation_validator():
    shelves = _module("shelves")
    vibes = _module("vibes")
    covers = _module("covers")
    undertone = _module("undertone")
    shelf = {"id": "m", "operation": "add", "member": {
        "id": "a", "entityId": "a", "kind": "album", "title": "A", "artist": "B", "addedAt": 1,
        "extra": math.nan}}
    vibe = {"id": "m", "operation": "put", "vibeId": "palette:x", "baseRevision": 0,
            "vibe": {"kind": "palette", "name": "X", "recipe": {"weight": math.nan}}}
    for module, body in ((shelves, shelf), (vibes, vibe)):
        with pytest.raises(ValueError, match="finite"):
            module.validate_mutation(body)
    # A finite body still validates.
    shelf["member"]["extra"] = 1.5
    shelves.validate_mutation(shelf)
    for module in (covers, undertone):
        assert "allow_nan=False" in open(module.__file__, encoding="utf-8").read()


# ------------------------------------------------------------------ 11. MusicBrainz cap


def test_musicbrainz_body_is_read_with_a_running_cap():
    mb = _module("credits_musicbrainz")
    read = []

    def endless(chunk_size):
        while True:
            read.append(chunk_size)
            yield b"x" * chunk_size

    response = SimpleNamespace(status_code=200, headers={}, iter_content=endless)
    with pytest.raises(mb.MusicBrainzDeferred, match="bounded size"):
        mb.Client._payload(None, response, "release", None, "release", None)
    assert len(read) * 64 * 1024 <= mb.MAX_RESPONSE_BYTES + 64 * 1024


def test_musicbrainz_streamed_body_is_parsed():
    mb = _module("credits_musicbrainz")
    body = json.dumps({"id": "x"}).encode()
    response = SimpleNamespace(status_code=200, headers={},
                               iter_content=lambda chunk_size: iter([body[:5], body[5:]]))
    assert mb.Client._payload(None, response, "release", None, "release", None) == {"id": "x"}
