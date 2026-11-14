"""Watchdog: fail loudly if the data has gone stale.

Catches the sneaky failure where every run is green but nothing new lands
(e.g. a bug that always takes the 'skip' branch). Exit code 1 -> red X in Actions
-> GitHub emails the repo owner.
"""
import sys
from datetime import date

from pipeline import store

MAX_DATA_AGE_DAYS = 2      # MTGJSON publishes daily; 2 days stale = something's wrong
MAX_HOURS_SINCE_RUN = 6    # the hourly job should have run several times by now


def main() -> int:
    con = store.connect()
    newest = con.execute("SELECT max(price_date) FROM prices").fetchall()[0][0]
    hours = con.execute("""
        SELECT date_diff('minute', max(started_at), current_timestamp) / 60.0
        FROM ingest_runs WHERE mode <> 'seed'
    """).fetchall()[0][0]
    failed = con.execute("""
        SELECT count(*) FROM ingest_runs
        WHERE status = 'failed' AND started_at > current_timestamp - INTERVAL 24 HOUR
    """).fetchall()[0][0]
    age = (date.today() - newest).days if newest else None

    print(f"newest price_date={newest} ({age}d old) | last run {hours}h ago | failures in 24h={failed}")
    problems = []
    if age is None or age > MAX_DATA_AGE_DAYS:
        problems.append(f"data is {age} days old")
    if hours is None or hours > MAX_HOURS_SINCE_RUN:
        problems.append(f"no ingest run in {hours} hours")
    if problems:
        print("UNHEALTHY:", "; ".join(problems))
        return 1
    print("healthy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
