"""cProfile one no-change projection and print the top functions (diagnostic)."""
import cProfile
import io
import os
import pstats
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402


def main():
    top = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import catalog_analysis, core_v3

    class Adapter(core_v3.AudioMuseV3Adapter):
        provider_module = None

    db = stub_host.get_db()
    profiler = cProfile.Profile()
    profiler.enable()
    catalog_analysis.project_analysis(server_id=stub_host.SERVER_ID, db=db, adapter=Adapter())
    profiler.disable()
    out = io.StringIO()
    pstats.Stats(profiler, stream=out).sort_stats("tottime").print_stats(top)
    print(out.getvalue())


if __name__ == "__main__":
    main()
