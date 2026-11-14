"""The recurring job. Run hourly by GitHub Actions:  python -m pipeline.ingest

Decides skip / daily / backfill, merges idempotently, and records every run
(including the ones that find nothing) in ingest_runs.
"""
import os
import sys
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa

from pipeline import store
from pipeline.mtgjson import download, fetch_meta, normalize, open_xz, to_batches

HISTORY_DAYS = 90   # how far back AllPrices.json reaches; older gaps are unrecoverable


def trigger_name() -> str:
    event = os.environ.get("GITHUB_EVENT_NAME")
    return {"schedule": "github-schedule", "workflow_dispatch": "github-manual"}.get(event, event or "local")


def load_rows(name: str, since: date) -> pa.Table:
    """Download one MTGJSON price file to a temp dir and keep rows on/after `since`."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        download(name, path)
        with open_xz(path) as f:
            rows = (r for r in normalize(f) if r[6] >= since)
            batches = list(to_batches(rows))
    if not batches:
        return pa.table({})
    return pa.Table.from_batches(batches)


def main() -> int:
    con = store.connect()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:6]
    store.start_run(con, run_id, "daily", trigger_name())
    try:
        meta = fetch_meta()
        build, build_date = meta["version"], date.fromisoformat(meta["date"])
        last = con.execute("SELECT max(price_date) FROM prices").fetchall()[0][0]

        already = con.execute("""
            SELECT count(*) FROM ingest_runs
            WHERE source_build = ? AND status = 'success' AND mode IN ('seed', 'daily', 'backfill')
        """, [build]).fetchall()[0][0]
        if already:
            msg = f"build {build} already loaded; newest price_date {last}"
            store.finish_run(con, run_id, "success", mode="skip", source_build=build, message=msg)
            print("skip:", msg)
            return 0

        gap_days = (build_date - last).days if last else None
        if last is None:
            raise RuntimeError("prices table is empty: run the seed first (seed/load_seed.py)")
        if gap_days <= 1:
            # Normal day. Include `last` again so same-day revisions get picked up.
            mode, table = "daily", load_rows("AllPricesToday.json.xz", since=last)
        else:
            # We missed one or more builds (runner outage, MTGJSON down, ...).
            mode, table = "backfill", load_rows("AllPrices.json.xz", since=last)
            if gap_days > HISTORY_DAYS:
                print(f"warning: {gap_days}-day gap exceeds the {HISTORY_DAYS}-day history window")

        if table.num_rows == 0:
            raise RuntimeError(f"build {build} contained no prices on/after {last}")
        counts = store.merge_prices(con, table, run_id)
        new_last = con.execute("SELECT max(price_date) FROM prices").fetchall()[0][0]
        msg = f"{mode}: {last} -> {new_last} (gap {gap_days}d)"
        store.finish_run(con, run_id, "success", mode=mode, source_build=build, message=msg, **counts)
        print(msg, counts)
        return 0
    except Exception as e:
        store.finish_run(con, run_id, "failed", message=f"{type(e).__name__}: {e}"[:1000])
        raise


if __name__ == "__main__":
    sys.exit(main())
