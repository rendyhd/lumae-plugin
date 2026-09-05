import json
from pathlib import Path
ops=json.loads(Path('private-dist/step9-recovery-ops.json').read_text())
for op in ops:
 p=Path(op['file']); data=p.read_bytes(); old=op['old'].encode(); new=op['new'].encode(); count=data.count(old)
 if count != 1: raise SystemExit(f'{p}: expected one match, found {count}')
 p.write_bytes(data.replace(old,new,1))
print('updated recovery assertion')
