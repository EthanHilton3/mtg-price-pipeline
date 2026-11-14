"""One-time bulk load: card dimension + 90-day price history, from local files only.

Run after download_history.py and flatten_prices.py. Safe to re-run: the
price load goes through the same idempotent MERGE as the daily job.
"""
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import store  # noqa: E402

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"


def load_cards(con, build: str) -> int:
    cards = (RAW / "csv" / "cards.csv").as_posix()
    sets = (RAW / "csv" / "sets.csv").as_posix()
    con.execute("BEGIN TRANSACTION")
    con.execute("DELETE FROM cards")   # dimension is replaced wholesale from one build
    con.execute(f"""
        INSERT INTO cards
        SELECT c.uuid, c.name, c.setCode, s.name, TRY_CAST(s.releaseDate AS DATE),
               c.number, c.rarity, c.type, TRY_CAST(c.manaValue AS DOUBLE),
               coalesce(TRY_CAST(c.isReserved AS BOOLEAN), false), ?
        FROM read_csv('{cards}', sample_size = -1, all_varchar = true) c
        LEFT JOIN read_csv('{sets}', sample_size = -1, all_varchar = true) s
          ON s.code = c.setCode
        QUALIFY row_number() OVER (PARTITION BY c.uuid) = 1
    """, [build])
    # Tokens (Treasure, Goblin, ...) have prices too but live in their own file.
    tokens = (RAW / "csv" / "tokens.csv").as_posix()
    con.execute(f"""
        INSERT INTO cards
        SELECT t.uuid, t.name, t.setCode, s.name, TRY_CAST(s.releaseDate AS DATE),
               t.number, 'token', t.type, 0, false, ?
        FROM read_csv('{tokens}', sample_size = -1, all_varchar = true) t
        LEFT JOIN read_csv('{sets}', sample_size = -1, all_varchar = true) s
          ON s.code = t.setCode
        WHERE t.uuid NOT IN (SELECT uuid FROM cards)
        QUALIFY row_number() OVER (PARTITION BY t.uuid) = 1
    """, [build])
    con.execute("COMMIT")
    return con.execute("SELECT count(*) FROM cards").fetchall()[0][0]


def main():
    build = json.loads((RAW / "build.json").read_text())
    seed = sorted(RAW.glob("*_prices_seed.parquet"))[-1].as_posix()
    con = store.connect()
    run_id = f"seed-{uuid.uuid4().hex[:8]}"
    store.start_run(con, run_id, "seed", "local", build["version"])

    t0 = time.time()
    n_cards = load_cards(con, build["version"])
    print(f"cards: {n_cards:,} rows ({time.time() - t0:.0f}s)")

    if "--cards-only" in sys.argv:
        store.finish_run(con, run_id, "success", mode="cards", message="card dimension refresh")
        return

    t0 = time.time()
    counts = store.merge_prices(con, f"read_parquet('{seed}')", run_id)
    print(f"prices: {counts} ({time.time() - t0:.0f}s)")
    store.finish_run(con, run_id, "success", message=f"seed from {Path(seed).name}", **counts)


if __name__ == "__main__":
    main()
