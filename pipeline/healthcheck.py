"""Watchdog: fail loudly if the data has gone stale.

Catches the sneaky failure where every run is green but nothing new lands
(e.g. a bug that always takes the 'skip' branch). Exit code 1 -> red X in Actions
-> GitHub emails the repo owner.

It queries `prices` itself (the source of truth) rather than trusting the
ingest job's own report or the derived status-page tables.
"""
import sys
from datetime import date

from pipeline import store

MAX_DATA_AGE_DAYS = 2      # MTGJSON publishes daily; 2 days stale = something's wrong
MAX_HOURS_SINCE_RUN = 6    # the hourly job should have run several times by now


def check(con) -> dict:
    newest = con.execute("SELECT max(price_date) FROM prices").fetchall()[0][0]
    hours = con.execute("""
        SELECT date_diff('minute', max(started_at), current_timestamp::TIMESTAMP) / 60.0
        FROM ingest_runs WHERE mode <> 'seed'
    """).fetchall()[0][0]
    failed = con.execute("""
        SELECT count(*) FROM ingest_runs
        WHERE status = 'failed' AND started_at > current_timestamp::TIMESTAMP - INTERVAL 24 HOUR
    """).fetchall()[0][0]
    age = (date.today() - newest).days if newest else None

    problems = []
    if age is None or age > MAX_DATA_AGE_DAYS:
        problems.append(f"newest prices are {age} days old")
    if hours is None or hours > MAX_HOURS_SINCE_RUN:
        problems.append(f"no ingest run in {hours} hours")
    return {"newest": newest, "age_days": age, "hours_since_run": hours,
            "failures_24h": failed, "problems": problems}


def main() -> int:
    h = check(store.connect())
    print(f"newest price_date={h['newest']} ({h['age_days']}d old) | "
          f"last run {h['hours_since_run']}h ago | failures in 24h={h['failures_24h']}")
    if h["problems"]:
        print("UNHEALTHY:", "; ".join(h["problems"]))
        return 1
    print("healthy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
