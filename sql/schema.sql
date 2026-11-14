-- MTG price pipeline: star schema in DuckDB / MotherDuck.
--
-- No PRIMARY KEY constraints on purpose: analytical stores (BigQuery, ClickHouse,
-- MotherDuck) generally don't enforce them, and a PK index over 66M+ composite keys
-- would cost GBs of memory. Uniqueness is enforced at write time instead, by the
-- MERGE in pipeline/store.py, and verified by sql/checks.sql.

-- Fact table: one row per card printing x vendor x retail/buylist x finish x day.
-- Natural key: (uuid, format, provider, price_type, finish, price_date)
CREATE TABLE IF NOT EXISTS prices (
    uuid        VARCHAR NOT NULL,   -- MTGJSON card-printing id -> cards.uuid
    format      VARCHAR NOT NULL,   -- paper | mtgo
    provider    VARCHAR NOT NULL,   -- tcgplayer, cardkingdom, cardmarket, manapool, cardhoarder
    currency    VARCHAR,            -- USD | EUR
    price_type  VARCHAR NOT NULL,   -- retail (what you pay) | buylist (what a store pays you)
    finish      VARCHAR NOT NULL,   -- normal | foil | etched
    price_date  DATE    NOT NULL,
    price       DOUBLE  NOT NULL,
    loaded_at   TIMESTAMP DEFAULT current_timestamp,  -- when this row last changed
    run_id      VARCHAR             -- which seed/ingest run wrote it (lineage)
);

-- Dimension table: what a uuid actually is.
CREATE TABLE IF NOT EXISTS cards (
    uuid          VARCHAR,
    name          VARCHAR,
    set_code      VARCHAR,
    set_name      VARCHAR,
    set_release   DATE,
    number        VARCHAR,
    rarity        VARCHAR,
    type_line     VARCHAR,
    mana_value    DOUBLE,
    is_reserved   BOOLEAN,          -- Reserved List: never reprinted, prices only go one way
    source_build  VARCHAR           -- MTGJSON build the row came from
);

-- Continuity ledger: one row per pipeline run, including runs that found nothing.
-- This is how we prove the pipeline is alive between daily price drops.
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id         VARCHAR,
    mode           VARCHAR,         -- seed | daily | backfill | skip
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    source_build   VARCHAR,         -- MTGJSON version, e.g. 5.3.0+20261112
    trigger        VARCHAR,         -- local | github-schedule | github-manual
    rows_fetched   BIGINT,
    rows_inserted  BIGINT,
    rows_updated   BIGINT,
    rows_unchanged BIGINT,
    unknown_cards  BIGINT,          -- price uuids with no row in cards yet
    status         VARCHAR,         -- running | success | failed
    message        VARCHAR
);
