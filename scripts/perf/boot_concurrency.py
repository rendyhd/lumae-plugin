"""Diagnostic: two concurrent v2 creators for the same source.

The first creator starts at t=0, the second after ``DELAY`` seconds (default
0.5). Before P1-6 a global advisory lock was held for the whole snapshot copy
and the second creator failed with 503 after the 5 s lock timeout; from P1-6
admission holds the global lock only briefly and the second creator waits for
the first capture on the per-source lock. Usage:
``boot_concurrency.py [DELAY] [PAGE_SIZE]``.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stub_host  # noqa: E402
from stub_host import T  # noqa: E402


def main():
    delay = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    page_size = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    stub_host.load_plugin()
    from plugins.LumaeAnalysis import profile_bootstrap as pb

    aux = stub_host.aux_cursor()
    src = stub_host.default_source(aux)
    aux.execute(f"DELETE FROM {T}profile_bootstrap_sessions")
    aux.execute(f"DELETE FROM {T}profile_bootstrap_creates")

    def body(**kw):
        return {"protocol_version": 2, "schema_version": 1,
                "transfer_contract": pb.TRANSFER_CONTRACT, "catalog_instance_id": src, **kw}

    results = {}

    def run(name, wait):
        time.sleep(wait)
        t0 = time.perf_counter()
        try:
            created = pb.create_session(body(page_size=page_size))
            outcome = "ok"
            pb.release_session(body(session_token=created["session_token"]))
        except pb.BootstrapError as exc:
            outcome = f"{exc.code}/{exc.status}"
        results[name] = {"result": outcome, "elapsed_s": round(time.perf_counter() - t0, 2)}

    threads = [threading.Thread(target=run, args=("first", 0)),
               threading.Thread(target=run, args=(f"second_after_{delay}s", delay))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stub_host.emit({"bench": "bootstrap_concurrency", "page_size": page_size, **results})


if __name__ == "__main__":
    main()
