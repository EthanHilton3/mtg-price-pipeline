"""Fetch and normalize MTGJSON price files.

Shared by the one-time seed and the recurring ingest job, so both produce
identical rows.

MTGJSON price files are nested as:
    data[uuid][format][provider] = {currency, retail: {finish: {date: price}},
                                              buylist: {finish: {date: price}}}
normalize() flattens that into one row per
    (uuid, format, provider, price_type, finish, price_date).
"""
import lzma
from datetime import date
from decimal import Decimal
from typing import BinaryIO, Iterator

import ijson
import pyarrow as pa
import requests

BASE = "https://mtgjson.com/api/v5"
HEADERS = {"User-Agent": "cs452-mtg-price-pipeline (student project)"}

SCHEMA = pa.schema([
    ("uuid", pa.string()),
    ("format", pa.string()),       # paper | mtgo
    ("provider", pa.string()),     # tcgplayer, cardkingdom, cardmarket, ...
    ("currency", pa.string()),     # USD | EUR
    ("price_type", pa.string()),   # retail | buylist
    ("finish", pa.string()),       # normal | foil | etched
    ("price_date", pa.date32()),
    ("price", pa.float64()),
])


def fetch_meta() -> dict:
    """Current MTGJSON build, e.g. {'date': '2026-11-12', 'version': '5.3.0+20261112'}."""
    r = requests.get(f"{BASE}/Meta.json", headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()["data"]


def download(name: str, dest) -> None:
    """Stream a file to disk, writing to .part first so a crash never leaves a half file."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(f"{BASE}/{name}", headers=HEADERS, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    tmp.replace(dest)


def normalize(fileobj: BinaryIO) -> Iterator[tuple]:
    """Stream (uuid, format, provider, currency, price_type, finish, date, price) rows.

    Uses ijson so the ~1 GB uncompressed history never has to fit in memory.
    """
    for uuid, formats in ijson.kvitems(fileobj, "data"):
        for fmt, providers in formats.items():
            for provider, p in providers.items():
                currency = p.get("currency")
                for price_type in ("retail", "buylist"):
                    for finish, series in (p.get(price_type) or {}).items():
                        for d, price in series.items():
                            if price is None:
                                continue
                            if isinstance(price, Decimal):
                                price = float(price)
                            yield (uuid, fmt, provider, currency, price_type, finish,
                                   date.fromisoformat(d), round(price, 4))


def to_batches(rows: Iterator[tuple], size: int = 500_000) -> Iterator[pa.RecordBatch]:
    """Group rows into Arrow record batches for fast parquet writes and DuckDB loads."""
    cols = [[] for _ in SCHEMA]
    for row in rows:
        for c, v in zip(cols, row):
            c.append(v)
        if len(cols[0]) >= size:
            yield pa.record_batch(cols, schema=SCHEMA)
            cols = [[] for _ in SCHEMA]
    if cols[0]:
        yield pa.record_batch(cols, schema=SCHEMA)


def open_xz(path) -> BinaryIO:
    return lzma.open(path, "rb")
