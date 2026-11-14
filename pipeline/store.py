"""DuckDB / MotherDuck access: connect, schema, idempotent MERGE, run ledger.

The same code talks to a local file (data/mtg.duckdb) or MotherDuck (md:mtg);
only the MTG_DB connection string changes.
"""
import json
import os
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parent.parent
SECRETS = ROOT.parent / "secrets.json"   # repo-root secrets.json (gitignored)


def connect() -> duckdb.DuckDBPyConnection:
    target = os.environ.get("MTG_DB", str(ROOT / "data" / "mtg.duckdb"))
    if target.startswith("md:") and "motherduck_token" not in os.environ:
        # Local runs: read the token from secrets.json into this process only.
        # In GitHub Actions it comes from a repo secret instead. Never printed.
        os.environ["motherduck_token"] = json.load(open(SECRETS))["motherduck"]["token"]
    if target.startswith("md:"):
        # MotherDuck needs the database to exist before we can attach to it.
        boot = duckdb.connect("md:")
        boot.execute(f"CREATE DATABASE IF NOT EXISTS {target[3:]}")
        boot.close()
    con = duckdb.connect(target)
    con.execute("SET enable_progress_bar = false")   # keeps CI logs readable
    # Laptop = Mountain time, GitHub runners = UTC. Pin one zone so the
    # timestamps both write into ingest_runs are comparable.
    con.execute("SET TimeZone = 'UTC'")
    # Big sorts/dedupes spill to disk. DuckDB names the spill folder after the
    # database path, which for "md:mtg" is an invalid Windows path, so set it.
    spill = (Path(tempfile.gettempdir()) / "mtg_duckdb_spill").as_posix()
    con.execute(f"SET temp_directory = '{spill}'")
    con.execute((ROOT / "sql" / "schema.sql").read_text())
    return con


def merge_prices(con, source, run_id: str) -> dict:
    """Upsert a batch of price rows. Safe to run any number of times on the same data.

    source: a pyarrow Table (daily run, ~765k rows) or a SQL expression such as
            "read_parquet('seed.parquet')" (seed, 66M rows, streamed by DuckDB).

    - new key                     -> INSERT
    - existing key, price changed -> UPDATE (vendors sometimes revise a day)
    - existing key, same price    -> left alone (counted as unchanged)
    """
    if isinstance(source, pa.Table):
        con.register("staging_arrow", source)
        source = "staging_arrow"
    # Dedupe inside the batch too, so a messy batch can't create duplicates on its own.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE staging AS
        SELECT uuid, format, provider, currency, price_type, finish, price_date, price
        FROM {source}
        QUALIFY row_number() OVER (
            PARTITION BY uuid, format, provider, price_type, finish, price_date) = 1
    """)
    if source == "staging_arrow":
        con.unregister("staging_arrow")

    # Only compare against the dates this batch covers, so a daily run scans
    # one day of the target instead of all 66M rows.
    lo, hi = con.execute("SELECT min(price_date), max(price_date) FROM staging").fetchall()[0]
    con.execute("""
        CREATE OR REPLACE TEMP TABLE diff AS
        SELECT s.*,
               CASE WHEN t.uuid IS NULL THEN 'insert'
                    WHEN t.price <> s.price THEN 'update'
                    ELSE 'same' END AS action
        FROM staging s
        LEFT JOIN (SELECT uuid, format, provider, price_type, finish, price_date, price
                   FROM prices WHERE price_date BETWEEN ? AND ?) t
          USING (uuid, format, provider, price_type, finish, price_date)
    """, [lo, hi])
    counts = dict(con.execute("SELECT action, count(*) FROM diff GROUP BY 1").fetchall())

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("""
            UPDATE prices t
            SET price = d.price, currency = d.currency,
                loaded_at = current_timestamp, run_id = ?
            FROM diff d
            WHERE d.action = 'update'
              AND t.price_date BETWEEN ? AND ?
              AND t.uuid = d.uuid AND t.format = d.format AND t.provider = d.provider
              AND t.price_type = d.price_type AND t.finish = d.finish
              AND t.price_date = d.price_date
        """, [run_id, lo, hi])
        con.execute("""
            INSERT INTO prices
            SELECT uuid, format, provider, currency, price_type, finish, price_date, price,
                   current_timestamp, ?
            FROM diff WHERE action = 'insert'
            ORDER BY price_date, uuid   -- keeps each day's rows together for fast date filters
        """, [run_id])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    unknown = con.execute("""
        SELECT count(DISTINCT s.uuid) FROM staging s
        ANTI JOIN cards c USING (uuid)
    """).fetchall()[0][0]
    return {
        "rows_fetched": sum(counts.values()),
        "rows_inserted": counts.get("insert", 0),
        "rows_updated": counts.get("update", 0),
        "rows_unchanged": counts.get("same", 0),
        "unknown_cards": unknown,
    }


def start_run(con, run_id, mode, trigger, source_build=None):
    con.execute("""INSERT INTO ingest_runs (run_id, mode, started_at, source_build, trigger, status)
                   VALUES (?, ?, current_timestamp, ?, ?, 'running')""",
                [run_id, mode, source_build, trigger])


def finish_run(con, run_id, status, mode=None, source_build=None, message=None, **counts):
    con.execute("""
        UPDATE ingest_runs SET
            finished_at = current_timestamp, status = ?, message = ?,
            mode = coalesce(?, mode), source_build = coalesce(?, source_build),
            rows_fetched = ?, rows_inserted = ?, rows_updated = ?, rows_unchanged = ?,
            unknown_cards = ?
        WHERE run_id = ?
    """, [status, message, mode, source_build,
          counts.get("rows_fetched"), counts.get("rows_inserted"), counts.get("rows_updated"),
          counts.get("rows_unchanged"), counts.get("unknown_cards"), run_id])
