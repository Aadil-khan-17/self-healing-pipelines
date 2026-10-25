"""Baseline (agent-free) pipeline on DuckDB: Bronze -> Silver -> Gold, with DQ gate + quarantine.

Deliberately "dumb": it uses a static alias table and one fixed date format, so it breaks on
schema drift and format changes. That is the point: Phase 2 agents replace the static parts
(mapping, date format, repair) while this deterministic core stays.

    python -m src.baseline --reset --all          # run days 1..5
    python -m src.baseline --day 3                # one batch
    python -m src.baseline --day 3 --date-format "%d/%m/%Y %H:%M"   # human override
"""
import argparse
import uuid
from datetime import datetime

import duckdb

from src.common import ALIASES, DB_PATH, ISO_FMT, LANDING_DIR, N_DAYS, REQUIRED
from src.ingest import FeedBatch, load_batch

QUARANTINE_FAIL_RATE = 0.20      # DQ gate: fail the batch if more than 20% of a feed is rejected
NULL_CUSTOMER_WARN_RATE = 0.30   # warn only: guest checkouts are legitimate

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (run_id VARCHAR, batch_id VARCHAR, status VARCHAR, started_at TIMESTAMP,
    finished_at TIMESTAMP, rows_bronze INTEGER, rows_silver INTEGER, rows_quarantined INTEGER, notes VARCHAR);
CREATE TABLE IF NOT EXISTS dq_results (batch_id VARCHAR, feed VARCHAR, check_name VARCHAR, passed BOOLEAN,
    observed DOUBLE, threshold DOUBLE, severity VARCHAR);
CREATE TABLE IF NOT EXISTS quarantine_rows (_batch_id VARCHAR, feed VARCHAR, _source_file VARCHAR, _row_num INTEGER,
    reason VARCHAR, raw VARCHAR);
CREATE TABLE IF NOT EXISTS silver_orders (row_hash VARCHAR PRIMARY KEY, invoice_no VARCHAR, stock_code VARCHAR,
    description VARCHAR, quantity INTEGER, invoice_ts TIMESTAMP, unit_price DECIMAL(10,2), customer_id INTEGER,
    country VARCHAR, is_cancellation BOOLEAN, _batch_id VARCHAR, _source_file VARCHAR);
CREATE TABLE IF NOT EXISTS silver_customers (customer_id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR,
    city VARCHAR, country VARCHAR, segment VARCHAR, updated_at TIMESTAMP, _batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS silver_returns (return_id VARCHAR PRIMARY KEY, invoice_no VARCHAR, stock_code VARCHAR,
    quantity INTEGER, return_ts TIMESTAMP, reason VARCHAR, _batch_id VARCHAR);
CREATE TABLE IF NOT EXISTS dim_customer (customer_sk BIGINT, customer_id INTEGER, name VARCHAR, email VARCHAR,
    city VARCHAR, country VARCHAR, segment VARCHAR, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN);
"""


# ----------------------------------------------------------------------------- helpers
def json_path(col: str) -> str:
    """'address.city' -> $."address"."city" (handles odd column names like 'Customer ID')."""
    return "$." + ".".join(f'"{p}"' for p in col.split("."))


def resolve_mapping(feed: str, source_cols: list[str]) -> tuple[dict, list[str], list[str]]:
    """Static alias lookup. Returns (canonical->source, missing_required, unmapped_source_cols)."""
    mapping = {}
    for canon, aliases in ALIASES[feed].items():
        hit = next((a for a in aliases if a in source_cols), None)
        if hit:
            mapping[canon] = hit
    missing = [c for c in REQUIRED[feed] if c not in mapping]
    unmapped = [c for c in source_cols if c not in mapping.values()
                and not any(m.startswith(c + ".") for m in mapping.values())]
    return mapping, missing, unmapped


def extract(m: dict, canon: str) -> str:
    return f"json_extract_string(raw, '{json_path(m[canon])}')" if canon in m else "CAST(NULL AS VARCHAR)"


def one(con, sql: str, params=None):
    return con.execute(sql, params or []).fetchone()[0]


def record_dq(con, batch, feed, check, passed, observed, threshold, severity):
    con.execute("INSERT INTO dq_results VALUES (?,?,?,?,?,?,?)",
                [batch, feed, check, bool(passed), float(observed), float(threshold), severity])


# ----------------------------------------------------------------------------- bronze
def write_bronze(con, fb: FeedBatch) -> int:
    tbl = f"bronze_{fb.feed}"
    con.execute(f"CREATE TABLE IF NOT EXISTS {tbl} (_batch_id VARCHAR, _source_file VARCHAR, _ingest_ts TIMESTAMP, "
                f"_row_num INTEGER, raw JSON)")
    con.execute(f"DELETE FROM {tbl} WHERE _batch_id = ?", [fb.batch_id])     # idempotent re-run
    frame = fb.frame
    con.register("_bf", frame)
    con.execute(f"INSERT INTO {tbl} SELECT _batch_id, _source_file, _ingest_ts, _row_num, CAST(raw AS JSON) FROM _bf")
    con.unregister("_bf")
    return len(frame)


# ----------------------------------------------------------------------------- staging (typed + validated, not yet persisted)
def stage_orders(con, batch: str, m: dict, fmt: str) -> None:
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE stg_orders AS
    WITH r AS (
      SELECT _batch_id, _source_file, _row_num, raw,
        NULLIF(TRIM({extract(m, 'invoice_no')}), '')  AS invoice_no,
        NULLIF(TRIM({extract(m, 'stock_code')}), '')  AS stock_code,
        NULLIF(TRIM({extract(m, 'description')}), '') AS description,
        {extract(m, 'quantity')}     AS qty_raw,
        {extract(m, 'invoice_date')} AS date_raw,
        {extract(m, 'unit_price')}   AS price_raw,
        {extract(m, 'customer_id')}  AS cust_raw,
        NULLIF(TRIM({extract(m, 'country')}), '') AS country
      FROM bronze_orders WHERE _batch_id = '{batch}'),
    t AS (
      SELECT *,
        TRY_CAST(TRY_CAST(qty_raw AS DOUBLE) AS INTEGER) AS quantity,
        TRY_CAST(price_raw AS DECIMAL(10,2))             AS unit_price,
        TRY_STRPTIME(date_raw, '{fmt}')                  AS invoice_ts,
        TRY_CAST(TRY_CAST(cust_raw AS DOUBLE) AS INTEGER) AS customer_id,
        COALESCE(invoice_no LIKE 'C%', false)            AS is_cancellation
      FROM r)
    SELECT *,
      md5(concat_ws('|', invoice_no, stock_code, quantity, invoice_ts, unit_price)) AS row_hash,
      CASE WHEN invoice_no IS NULL THEN 'null_invoice_no'
           WHEN stock_code IS NULL THEN 'null_stock_code'
           WHEN quantity   IS NULL THEN 'bad_quantity'
           WHEN invoice_ts IS NULL THEN 'bad_invoice_date'
           WHEN unit_price IS NULL THEN 'bad_unit_price'
           WHEN unit_price < 0     THEN 'negative_price'
           WHEN quantity <= 0 AND NOT is_cancellation THEN 'nonpositive_qty_on_sale'
      END AS reject_reason
    FROM t""")


def stage_customers(con, batch: str, m: dict) -> None:
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE stg_customers AS
    WITH r AS (
      SELECT _batch_id, _source_file, _row_num, raw,
        TRY_CAST({extract(m, 'customer_id')} AS INTEGER) AS customer_id,
        {extract(m, 'name')} AS name, {extract(m, 'email')} AS email,
        {extract(m, 'city')} AS city, {extract(m, 'country')} AS country, {extract(m, 'segment')} AS segment,
        TRY_CAST({extract(m, 'updated_at')} AS TIMESTAMP) AS updated_at
      FROM bronze_customers WHERE _batch_id = '{batch}')
    SELECT *, CASE WHEN customer_id IS NULL THEN 'null_customer_id'
                   WHEN updated_at  IS NULL THEN 'bad_updated_at' END AS reject_reason
    FROM r""")


def stage_returns(con, batch: str, m: dict, fmt: str) -> None:
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE stg_returns AS
    WITH r AS (
      SELECT _batch_id, _source_file, _row_num, raw,
        NULLIF(TRIM({extract(m, 'return_id')}), '') AS return_id,
        NULLIF(TRIM({extract(m, 'invoice_no')}), '') AS invoice_no,
        NULLIF(TRIM({extract(m, 'stock_code')}), '') AS stock_code,
        TRY_CAST(TRY_CAST({extract(m, 'quantity')} AS DOUBLE) AS INTEGER) AS quantity,
        TRY_STRPTIME({extract(m, 'return_date')}, '{fmt}') AS return_ts,
        {extract(m, 'reason')} AS reason
      FROM bronze_returns WHERE _batch_id = '{batch}')
    SELECT *, CASE WHEN return_id IS NULL THEN 'null_return_id'
                   WHEN quantity IS NULL OR quantity <= 0 THEN 'bad_quantity'
                   WHEN return_ts IS NULL THEN 'bad_return_date' END AS reject_reason
    FROM r""")


# ----------------------------------------------------------------------------- DQ gate
def run_dq(con, batch: str, feed: str, rejected_lines: int) -> list[str]:
    """Record checks, return list of failed 'fail'-severity checks."""
    stg = f"stg_{feed}"
    total = one(con, f"SELECT count(*) FROM {stg}")
    rejected = one(con, f"SELECT count(*) FROM {stg} WHERE reject_reason IS NOT NULL")
    rate = rejected / total if total else 0.0
    failures = []
    ok = rate <= QUARANTINE_FAIL_RATE
    record_dq(con, batch, feed, "quarantine_rate", ok, rate, QUARANTINE_FAIL_RATE, "fail")
    if not ok:
        failures.append(f"{feed}: quarantine rate {rate:.0%} > {QUARANTINE_FAIL_RATE:.0%}")
    record_dq(con, batch, feed, "unparseable_lines", rejected_lines == 0, rejected_lines, 0, "warn")
    if feed == "orders":
        null_cust = one(con, "SELECT avg((customer_id IS NULL)::INT) FROM stg_orders WHERE reject_reason IS NULL") or 0
        record_dq(con, batch, feed, "null_customer_rate", null_cust <= NULL_CUSTOMER_WARN_RATE, null_cust,
                  NULL_CUSTOMER_WARN_RATE, "warn")
        dups = one(con, "SELECT count(*) - count(DISTINCT row_hash) FROM stg_orders WHERE reject_reason IS NULL")
        record_dq(con, batch, feed, "duplicate_rows_in_batch", dups == 0, dups, 0, "warn")
    return failures


# ----------------------------------------------------------------------------- silver + gold
def write_quarantine(con, batch: str, feed: str) -> int:
    con.execute("DELETE FROM quarantine_rows WHERE _batch_id = ? AND feed = ?", [batch, feed])
    con.execute(f"INSERT INTO quarantine_rows SELECT _batch_id, '{feed}', _source_file, _row_num, reject_reason, "
                f"CAST(raw AS VARCHAR) FROM stg_{feed} WHERE reject_reason IS NOT NULL")
    return one(con, "SELECT count(*) FROM quarantine_rows WHERE _batch_id = ? AND feed = ?", [batch, feed])


def write_silver(con, batch: str) -> int:
    before = one(con, "SELECT (SELECT count(*) FROM silver_orders) + (SELECT count(*) FROM silver_returns)")
    con.execute("""
    INSERT OR IGNORE INTO silver_orders
    SELECT row_hash, invoice_no, stock_code, description, quantity, invoice_ts, unit_price, customer_id, country,
           is_cancellation, _batch_id, _source_file
    FROM stg_orders WHERE reject_reason IS NULL
    QUALIFY row_number() OVER (PARTITION BY row_hash ORDER BY _source_file, _row_num) = 1""")
    con.execute("""
    INSERT OR REPLACE INTO silver_customers
    SELECT customer_id, name, email, city, country, segment, updated_at, _batch_id
    FROM stg_customers WHERE reject_reason IS NULL
    QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1""")
    con.execute("""
    INSERT OR IGNORE INTO silver_returns
    SELECT return_id, invoice_no, stock_code, quantity, return_ts, reason, _batch_id
    FROM stg_returns WHERE reject_reason IS NULL
    QUALIFY row_number() OVER (PARTITION BY return_id ORDER BY _source_file, _row_num) = 1""")
    after = one(con, "SELECT (SELECT count(*) FROM silver_orders) + (SELECT count(*) FROM silver_returns)")
    return after - before


def build_gold(con) -> None:
    # --- SCD Type 2 on customers: close changed rows, then insert new + changed versions
    con.execute("""
    CREATE OR REPLACE TEMP TABLE stg_cust_latest AS
    SELECT * FROM stg_customers WHERE reject_reason IS NULL
    QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1""")
    con.execute("""
    UPDATE dim_customer SET valid_to = s.updated_at, is_current = false
    FROM stg_cust_latest s
    WHERE dim_customer.customer_id = s.customer_id AND dim_customer.is_current
      AND (dim_customer.name    IS DISTINCT FROM s.name    OR dim_customer.email   IS DISTINCT FROM s.email
        OR dim_customer.city    IS DISTINCT FROM s.city    OR dim_customer.country IS DISTINCT FROM s.country
        OR dim_customer.segment IS DISTINCT FROM s.segment)""")
    con.execute("""
    INSERT INTO dim_customer
    SELECT (SELECT COALESCE(max(customer_sk), 0) FROM dim_customer) + row_number() OVER (ORDER BY s.customer_id),
           s.customer_id, s.name, s.email, s.city, s.country, s.segment, s.updated_at, NULL, true
    FROM stg_cust_latest s
    WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id AND d.is_current)""")
    # --- rebuilt-each-run gold tables (fine at this size; incremental MERGE is a stretch goal)
    con.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT DISTINCT CAST(strftime(CAST(invoice_ts AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
           CAST(invoice_ts AS DATE) AS date, year(invoice_ts) AS year, month(invoice_ts) AS month,
           day(invoice_ts) AS day, dayname(invoice_ts) AS weekday
    FROM silver_orders""")
    con.execute("""
    CREATE OR REPLACE TABLE dim_product AS
    SELECT stock_code, arg_max(description, invoice_ts) FILTER (WHERE description IS NOT NULL) AS description,
           round(avg(unit_price), 2) AS avg_unit_price
    FROM silver_orders GROUP BY stock_code""")
    con.execute("""
    CREATE OR REPLACE TABLE fact_sales AS
    SELECT row_hash AS sales_id, invoice_no, stock_code, customer_id,
           CAST(strftime(CAST(invoice_ts AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
           quantity, unit_price, quantity * unit_price AS line_amount, is_cancellation
    FROM silver_orders""")
    con.execute("CREATE OR REPLACE TABLE fact_returns AS SELECT * FROM silver_returns")


# ----------------------------------------------------------------------------- orchestration
def run_batch(con, day: int, date_fmt: str = ISO_FMT, mapping_overrides: dict | None = None) -> dict:
    batch = f"day_{day:02d}"
    run_id, started = str(uuid.uuid4())[:8], datetime.now()
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,NULL,0,0,0,'')", [run_id, batch, "RUNNING", started])
    notes, failures, n_bronze, n_quar, n_silver = [], [], 0, 0, 0

    def finish(status: str) -> dict:
        con.execute("UPDATE pipeline_runs SET status=?, finished_at=?, rows_bronze=?, rows_silver=?, "
                    "rows_quarantined=?, notes=? WHERE run_id=?",
                    [status, datetime.now(), n_bronze, n_silver, n_quar, " | ".join(notes + failures), run_id])
        return {"batch": batch, "status": status, "bronze": n_bronze, "silver_new": n_silver,
                "quarantined": n_quar, "notes": notes + failures}

    feeds = load_batch(LANDING_DIR / batch, batch)
    mappings = {}
    for feed, fb in feeds.items():
        n_bronze += write_bronze(con, fb)
        if mapping_overrides and feed in mapping_overrides:
            m = mapping_overrides[feed]
            missing = [c for c in REQUIRED[feed] if c not in m]
            unmapped = [c for c in fb.columns if c not in m.values()]
        else:
            m, missing, unmapped = resolve_mapping(feed, fb.columns)
        mappings[feed] = m
        if missing:
            failures.append(f"{feed}: unmapped required columns {missing} (source has {fb.columns})")
        if unmapped:
            notes.append(f"{feed}: ignoring unmapped source columns {unmapped}")
        if fb.rejected_lines:
            notes.append(f"{feed}: {fb.rejected_lines} unparseable line(s) skipped")
    if failures:                                           # schema drift the static mapping can't handle
        return finish("FAILED")

    # NOTE: this baseline assumes all three feeds arrive every day (true for the generated data).
    stage_orders(con, batch, mappings["orders"], date_fmt)
    stage_customers(con, batch, mappings["customers"])
    stage_returns(con, batch, mappings["returns"], ISO_FMT)   # returns dates are never corrupted by chaos

    for feed, fb in feeds.items():
        failures += run_dq(con, batch, feed, fb.rejected_lines)
    if failures:                                           # gate: nothing reaches silver/gold
        top = con.execute("SELECT reject_reason, count(*) FROM stg_orders WHERE reject_reason IS NOT NULL "
                          "GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall() if "orders" in feeds else []
        notes.append(f"top reject reasons: {top}")
        return finish("FAILED")

    for feed in feeds:
        n_quar += write_quarantine(con, batch, feed)
    n_silver = write_silver(con, batch)
    build_gold(con)
    return finish("SUCCESS")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", type=int)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--reset", action="store_true", help="delete the warehouse first")
    ap.add_argument("--date-format", default=ISO_FMT, help="strptime format for order dates")
    a = ap.parse_args()
    if a.reset and DB_PATH.exists():
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    days = range(1, N_DAYS + 1) if a.all else [a.day]
    for d in days:
        r = run_batch(con, d, a.date_format)
        print(f"[{r['batch']}] {r['status']:8} bronze={r['bronze']} new_silver={r['silver_new']} quarantined={r['quarantined']}")
        for n in r["notes"]:
            print("    -", n)


if __name__ == "__main__":
    main()
