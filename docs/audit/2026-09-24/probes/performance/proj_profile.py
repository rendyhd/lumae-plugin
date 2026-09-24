import cProfile, pstats, sys, io
sys.path.insert(0, ".")
import stub_host
stub_host.load_plugin()
from plugins.LumaeAnalysis import catalog_analysis, core_v3
class Adapter(core_v3.AudioMuseV3Adapter):
    provider_module = None
db = stub_host.get_db()
pr = cProfile.Profile()
pr.enable()
catalog_analysis.project_analysis(server_id="server-a", db=db, adapter=Adapter())
pr.disable()
s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("tottime").print_stats(18)
print(s.getvalue()[:6000])
