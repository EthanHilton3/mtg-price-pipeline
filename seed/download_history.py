"""One-time download of the MTGJSON price history and card metadata.

Seeding etiquette: pull once, keep the raw files in data/raw/, and bulk-load
from disk. Re-running skips files that are already downloaded.
"""
import json
import sys
from pathlib import Path

import requests

BASE = "https://mtgjson.com/api/v5"
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
HEADERS = {"User-Agent": "cs452-mtg-price-pipeline (student project)"}
FILES = ["AllPrices.json.xz", "AllPrintingsCSVFiles.tar.xz"]


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    build = requests.get(f"{BASE}/Meta.json", headers=HEADERS, timeout=30).json()["data"]
    print(f"MTGJSON build {build['version']} ({build['date']})")

    for name in FILES:
        dest = RAW / f"{build['date']}_{name}"
        if dest.exists():
            print(f"skip   {dest.name} (already downloaded)")
            continue
        tmp = dest.with_suffix(dest.suffix + ".part")
        with requests.get(f"{BASE}/{name}", headers=HEADERS, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
        tmp.rename(dest)
        print(f"saved  {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")

    (RAW / "build.json").write_text(json.dumps(build))


if __name__ == "__main__":
    sys.exit(main())
