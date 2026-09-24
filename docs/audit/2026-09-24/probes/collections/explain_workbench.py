import importlib.util, pathlib, sys, time, types, re
import psycopg2
plugin_module = types.ModuleType("plugin"); api = types.ModuleType("plugin.api")
api.config = types.SimpleNamespace(); api.get_db = lambda: None
api.logger = types.SimpleNamespace(warning=print, exception=print); api.table = lambda n: n
sys.modules["plugin"] = plugin_module; sys.modules["plugin.api"] = api
src = pathlib.Path("/home/user/lumae-plugin/plugins/LumaeAnalysis/collection_library.py")
spec = importlib.util.spec_from_file_location("cl", src); lib = importlib.util.module_from_spec(spec); spec.loader.exec_module(lib)
conn = psycopg2.connect("postgresql://postgres@127.0.0.1:55441/audit_collections")
with conn.cursor() as c:
    c.execute("SET search_path TO wb, public"); c.execute("SET work_mem='4MB'")
conn.commit()
plans = []
class Cur:
    def __init__(s): s.c = conn.cursor()
    def execute(s, sql, params=None):
        with conn.cursor() as e:
            e.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params)
            plan = [r[0] for r in e.fetchall()]
        plans.append(plan)
        s.c.execute(sql, params)
    def __getattr__(s, n): return getattr(s.c, n)
    def close(s): s.c.close()
class DB:
    def cursor(s): return Cur()
lib.get_db = lambda: DB()
def summarize(label, fn):
    plans.clear(); t0 = time.perf_counter(); out = fn(); dt = time.perf_counter() - t0
    for p in plans:
        exe = [l for l in p if "Execution Time" in l][0].strip()
        seq = sorted(set(re.findall(r"Seq Scan on (\w+)", "\n".join(p))))
        sort = [l.strip() for l in p if "Sort Method" in l][:2]
        print(f"{label}: {exe}; seqscans={seq}; {sort}")
    return out
r = summarize("albums p1 title", lambda: lib.browse_library("albums", "", None, "title", 1, 36))
print("   albums total:", r["sections"]["albums"]["total"])
summarize("albums p200 title", lambda: lib.browse_library("albums", "", None, "title", 200, 36))
r = summarize("albums p1 year", lambda: lib.browse_library("albums", "", None, "year", 1, 5))
print("   year sort first rows:", [(i["title"][:14], i["year"]) for i in r["sections"]["albums"]["items"]])
summarize("tracks p1 title", lambda: lib.browse_library("tracks", "", None, "title", 1, 36))
summarize("tracks p2000 title", lambda: lib.browse_library("tracks", "", None, "title", 2000, 36))
r = summarize("all q=love", lambda: lib.browse_library("all", "love", None, "title", 1, 36))
print("   totals:", {k: v["total"] for k, v in r["sections"].items()})
summarize("tracks q='love night'", lambda: lib.browse_library("tracks", "love night", None, "title", 1, 36))
summarize("artists p1", lambda: lib.browse_library("artists", "", None, "title", 1, 36))
summarize("stats", lib.library_stats)
# pick an album with a same-name second edition
with conn.cursor() as c:
    c.execute("SELECT name, album_artist_display FROM wb.catalog_albums WHERE album_id='al-ed-7'")
    name, artist = c.fetchone()
r = summarize("album_detail merged", lambda: lib.album_detail(name, artist))
print("   album_detail tracks:", len(r["tracks"]), "album_ids:", sorted({t["album_id"] for t in r["tracks"]}), "chosen provider_album_id:", r["album"]["provider_album_id"])
r = lib.browse_library("albums", name.split()[1][:8], None, "title", 1, 36)
print("   browse rows for that title:", [(i["title"][:14], i["artist"], i["track_count"], i["provider_album_id"]) for i in r["sections"]["albums"]["items"]])
