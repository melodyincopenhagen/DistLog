"""End-to-end example: start a distlog server, ingest some records,
run a few queries. Demonstrates the typed exceptions and backpressure
retry behavior.

Usage:
    # Terminal 1:
    ./bin/distlog --config configs/dev.yaml

    # Terminal 2:
    python3 examples/python/example.py

If you've enabled auth in configs/dev.yaml, pass DISTLOG_TOKEN=<token>
in the environment.
"""

from __future__ import annotations

import os
import sys
import time

from distlog_client import (
    BackpressureError,
    Client,
    ParseError,
    RetryPolicy,
    UnauthorizedError,
)

URL = os.environ.get("DISTLOG_URL", "http://localhost:8080")
TOKEN = os.environ.get("DISTLOG_TOKEN")


def main() -> int:
    client = Client(
        URL,
        token=TOKEN,
        retry=RetryPolicy(max_retries=5, initial_backoff_s=0.1, max_backoff_s=2.0),
    )

    print(f"-> connecting to {URL}")
    print("   healthz:", client.healthz()["status"])

    print("-> ingesting 5 records")
    ids = []
    for i in range(5):
        try:
            doc_id = client.write(
                f"request {i} handled",
                source=f"api-{i % 2}",
                fields={"level": "info" if i % 2 == 0 else "error"},
            )
            ids.append(doc_id)
        except UnauthorizedError:
            print("   server requires auth; set DISTLOG_TOKEN", file=sys.stderr)
            return 1
        except BackpressureError as e:
            print(f"   give up after retries: {e}", file=sys.stderr)
            return 1
    print(f"   doc_ids: {ids}")

    print("-> fetch one back")
    rec = client.get(ids[0])
    print(f"   {rec}")

    print("-> query: all messages with level=error")
    try:
        result = client.query(
            "SELECT message, source FROM logs WHERE fields.level = 'error' LIMIT 10"
        )
    except ParseError as e:
        print(f"   bad SQL: {e}", file=sys.stderr)
        return 1
    print(f"   {len(result.rows)} row(s); columns={result.columns}")
    for row in result.rows:
        print(f"   doc_id={row.doc_id} {row.values}")

    print("-> query with ts pushdown (planner extracts the bounds)")
    result = client.query(
        f"SELECT message FROM logs WHERE ts >= '2020-01-01T00:00:00Z' AND ts <= '2030-01-01T00:00:00Z' LIMIT 50"
    )
    print(f"   {len(result.rows)} row(s)")

    print("-> healthz after work")
    health = client.healthz()
    print(f"   sstables={health['sstable_count']} "
          f"active_memtable={health['active_memtable_size']} B "
          f"pruned_last_scan={health['last_scan_pruned_sstables']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
