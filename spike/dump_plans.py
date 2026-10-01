"""Save the CompileTimeLQP returned by a running LqpServer for each query, to diff servers.

usage: python3 spike/dump_plans.py <out-dir> <sql-file-or-dir> [...]
Writes <out-dir>/<query-name>.json (CompileTimeLQP only, keys sorted) or .error.
"""
import http.client
import json
import sys
from pathlib import Path


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for arg in sys.argv[2:]:
        p = Path(arg)
        files += sorted(p.glob("*.sql")) if p.is_dir() else [p]
    conn = http.client.HTTPConnection("localhost", 8080)
    ok = 0
    for f in files:
        conn.request("POST", "/lqp", json.dumps({"sql": f.read_text()}), {"Content-Type": "application/json"})
        resp = conn.getresponse()
        body = resp.read().decode()
        if resp.status == 200:
            plan = json.loads(body)["CompileTimeLQP"]
            (out / f"{f.stem}.json").write_text(json.dumps(plan, indent=1, sort_keys=True))
            ok += 1
        else:
            (out / f"{f.stem}.error").write_text(body)
    print(f"{ok}/{len(files)} plans saved to {out}")


if __name__ == "__main__":
    main()
