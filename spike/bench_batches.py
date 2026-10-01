"""Send batches of queries to a running LqpServer over one keep-alive connection.

usage: python3 spike/bench_batches.py <n> <sql-dir> [<sql-dir> ...]
Each directory is one batch; n files are picked evenly spread across it.
"""
import http.client
import json
import statistics
import sys
import time
from pathlib import Path


def pick(directory: str, n: int) -> list[Path]:
    files = sorted(Path(directory).glob("*.sql"))
    step = len(files) / n
    return [files[int(i * step)] for i in range(n)]


def run_batch(conn: http.client.HTTPConnection, files: list[Path]) -> None:
    client_ms, planning_ms, failures = [], [], 0
    start = time.perf_counter()
    for f in files:
        body = json.dumps({"sql": f.read_text()})
        t = time.perf_counter()
        conn.request("POST", "/lqp", body, {"Content-Type": "application/json"})
        resp = conn.getresponse()
        resp.read()
        client_ms.append((time.perf_counter() - t) * 1000)
        if resp.status != 200:
            failures += 1
        elif resp.getheader("X-Planning-Time-Ms"):
            planning_ms.append(float(resp.getheader("X-Planning-Time-Ms")))
    wall = time.perf_counter() - start
    c = sorted(client_ms)
    print(
        f"  wall {wall:.2f} s | failures {failures} | per query ms: "
        f"mean {statistics.mean(c):.0f}, p50 {c[len(c) // 2]:.0f}, "
        f"p90 {c[int(len(c) * 0.9)]:.0f}, max {c[-1]:.0f} | "
        f"planning mean {statistics.mean(planning_ms):.0f}"
    )


def main() -> None:
    n = int(sys.argv[1])
    conn = http.client.HTTPConnection("localhost", 8080)
    for i, directory in enumerate(sys.argv[2:], 1):
        print(f"batch {i}: {n} queries from {Path(directory).name}")
        run_batch(conn, pick(directory, n))


if __name__ == "__main__":
    main()
