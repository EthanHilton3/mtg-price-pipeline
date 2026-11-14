-- Paste these into the MotherDuck UI (database: mtg).

-- 1. Liveness: every run, newest first. 'skip' rows prove the hourly job is
--    firing even on hours with no new MTGJSON build.
SELECT started_at, trigger, mode, status, rows_inserted, rows_updated, rows_unchanged, message
FROM ingest_runs ORDER BY started_at DESC LIMIT 25;

-- 2. Growth: rows per price_date. Each new day appearing on its own = alive.
SELECT price_date, count(*) AS rows FROM prices GROUP BY 1 ORDER BY 1 DESC LIMIT 14;

-- 3. Idempotency: total rows must equal distinct natural keys (0 duplicates).
SELECT count(*) AS total_rows,
       count(DISTINCT (uuid, format, provider, price_type, finish, price_date)) AS distinct_keys
FROM prices;

-- 4. Continuity: days missing between the first and last price_date (want 0 rows).
SELECT d::DATE AS missing_day
FROM range((SELECT min(price_date) FROM prices), (SELECT max(price_date) FROM prices), INTERVAL 1 DAY) t(d)
WHERE d::DATE NOT IN (SELECT DISTINCT price_date FROM prices);

-- 5. Live vs. history join: today's TCGplayer price vs. each card's 90-day average.
--    Biggest risers among cards worth >= $5.
WITH latest AS (SELECT max(price_date) AS d FROM prices),
p AS (
    SELECT uuid, finish, price_date, price FROM prices
    WHERE provider = 'tcgplayer' AND price_type = 'retail' AND format = 'paper'
)
SELECT c.name, c.set_name, p.finish,
       round(avg(p.price) FILTER (WHERE p.price_date < l.d), 2) AS avg_90d,
       max(p.price) FILTER (WHERE p.price_date = l.d)          AS today,
       round(100 * (today / avg_90d - 1), 1)                   AS pct_vs_avg
FROM p CROSS JOIN latest l JOIN cards c USING (uuid)
GROUP BY ALL
HAVING today >= 5 AND avg_90d > 0
ORDER BY pct_vs_avg DESC
LIMIT 20;
