import json, sys, threading, time
sys.path.insert(0, ".")
import stub_host
stub_host.load_plugin()
from plugins.LumaeAnalysis import profile_bootstrap as pb
aux = stub_host.connect(); aux.autocommit = True; ac = aux.cursor()
ac.execute("SELECT catalog_instance_id FROM plugin_lumae_analysis__catalog_sources"); SRC = ac.fetchone()[0]
body = lambda **kw: {"protocol_version": 2, "schema_version": 1, "transfer_contract": pb.TRANSFER_CONTRACT, "catalog_instance_id": SRC, **kw}
res = {}
def run(name, delay):
    time.sleep(delay); t0 = time.perf_counter()
    try:
        r = pb.create_session(body(page_size=500)); out = "ok"
        pb.release_session(body(session_token=r["session_token"]))
    except pb.BootstrapError as e:
        out = f"{e.code}/{e.status}"
    res[name] = {"result": out, "s": round(time.perf_counter() - t0, 2)}
ts = [threading.Thread(target=run, args=("first", 0)), threading.Thread(target=run, args=("second_after_0.5s", 0.5))]
[t.start() for t in ts]; [t.join() for t in ts]
print(json.dumps(res))
