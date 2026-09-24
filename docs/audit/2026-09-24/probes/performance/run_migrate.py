import time, sys
sys.path.insert(0, '.')
import stub_host
mod = stub_host.load_plugin()
db = stub_host.connect()
t0 = time.perf_counter()
try:
    mod.migrate(db)
except Exception as e:
    import traceback; traceback.print_exc()
    db.rollback()
print("migrate empty-db seconds", round(time.perf_counter()-t0, 3))
