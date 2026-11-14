"""Flatten the downloaded AllPrices.json.xz into parquet (bronze -> silver).

Reads only from data/raw/; never touches the network.
"""
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.mtgjson import SCHEMA, normalize, open_xz, to_batches  # noqa: E402

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"


def main():
    src = sorted(RAW.glob("*_AllPrices.json.xz"))[-1]
    out = RAW / src.name.replace("_AllPrices.json.xz", "_prices_seed.parquet")
    t0, n = time.time(), 0
    with open_xz(src) as f, pq.ParquetWriter(out, SCHEMA, compression="zstd") as w:
        for batch in to_batches(normalize(f)):
            w.write_batch(batch)
            n += batch.num_rows
            print(f"\r{n:,} rows  {time.time() - t0:.0f}s", end="", flush=True)
    print(f"\nwrote {out.name}: {n:,} rows, {out.stat().st_size / 1e6:.1f} MB, {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
