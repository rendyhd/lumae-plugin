import sys, runpy
sys.path.insert(0, ".")
import stub_host
stub_host.load_plugin()
from plugins.LumaeAnalysis import catalog_readiness as cr
cov = {"eligible_track_count":132000,"mapped_track_count":76000,"missing_mapping_count":56000,"chromaprint_track_count":76000,"chromaprint_missing_count":0,"chromaprint_coverage":1.0,"latest_chromaprint_at_unix":0.0}
links = {"ready_link_count":76000,"pending_link_count":0,"suspect_link_count":14000,"missing_link_count":56000,"evidence_complete_link_count":62000,"verified_link_count":62000,"provisional_link_count":0,"usable_analysis_coverage":0.57}
cr._coverage = lambda db, source: dict(cov)
cr._link_coverage = lambda db, source, n=0: dict(links)
sys.argv = ["route_bench.py", "/api/catalog/health", "30"]
runpy.run_path("route_bench.py", run_name="__main__")
