"""Chaos script: turn the clean feeds into realistic "bad days" and record exactly what was broken.

Changes are CUMULATIVE, like real sources (once a vendor renames a column, it stays renamed):

  day 1  clean baseline
  day 2  schema drift: renamed columns (orders: quantity->qty, unit_price->price, ...; returns: quantity->qty_returned, ...)
    day 3  + renamed columns, new columns, date format DD/MM/YYYY HH:MM, quantity as "3.0"
  day 4  + data defects: 5% null unit_price, 2% negative qty on sales, 5% null descriptions, 5% duplicate rows
  day 5  + file corruption: orders split into 2 files, one with a garbage line and a truncated last row

The ground-truth JSON is what the agents are scored against in Phase 3.

Usage:  python -m scripts.chaos
"""
import json
import random
import shutil
from pathlib import Path

import pandas as pd

from src.common import CLEAN_DIR, ISO_FMT, LANDING_DIR, N_DAYS, TRUTH_DIR

ORDER_RENAMES = {"quantity": "qty", "unit_price": "price", "invoice_date": "InvoiceDate", "customer_id": "CustomerID"}
RETURN_RENAMES = {"quantity": "qty_returned", "return_date": "returned_on"}
DIRTY_DATE_FMT = "%d/%m/%Y %H:%M"


def _keys(df: pd.DataFrame, idx) -> list[str]:
    sub = df.loc[list(idx)]
    return (sub.invoice_no + "|" + sub.stock_code).tolist()


def apply_day(day: int, clean_dir: Path, landing_dir: Path, truth_dir: Path, seed: int = 7) -> dict:
    rng = random.Random(seed * 100 + day)
    src, dst = clean_dir / f"day_{day:02d}", landing_dir / f"day_{day:02d}"
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    orders = pd.read_csv(src / "orders.csv", dtype=str, keep_default_na=False)
    customers = json.loads((src / "customers.json").read_text())
    returns = pd.read_excel(src / "returns.xlsx", dtype=str)

    truth: dict = {"day": day, "orders_renames": {}, "returns_renames": {}, "new_columns": {},
                   "orders_date_format": ISO_FMT, "defects": {}, "corruption": {}}

    # ---- day 4: data defects (disjoint rows so every defect is attributable)
    if day == 4:
        n = len(orders)
        pool = [i for i in range(n) if not orders.invoice_no[i].startswith("C")]
        rng.shuffle(pool)

        def take(frac: float) -> list[int]:
            k, out = int(frac * n), pool[: int(frac * n)]
            del pool[:k]
            return out

        null_price, neg_qty, null_desc, dup_src = take(.05), take(.02), take(.05), take(.05)
        orders.loc[null_price, "unit_price"] = ""
        orders.loc[neg_qty, "quantity"] = orders.loc[neg_qty, "quantity"].map(lambda q: str(-int(q)))
        orders.loc[null_desc, "description"] = ""
        truth["defects"] = {
            "null_unit_price": _keys(orders, null_price),
            "negative_qty_on_sale": _keys(orders, neg_qty),
            "null_description": _keys(orders, null_desc),
            "duplicate_extra_rows": _keys(orders, dup_src),
        }
        orders = pd.concat([orders, orders.loc[dup_src]]).sample(frac=1, random_state=day).reset_index(drop=True)
        # rows the pipeline must quarantine = null price + negative qty on a sale
        truth["expected_quarantine_keys"] = truth["defects"]["null_unit_price"] + truth["defects"]["negative_qty_on_sale"]

    # ---- day 3: date format + float-ish quantities + new columns
    if day == 3:
        ts = pd.to_datetime(orders.invoice_date, format=ISO_FMT, errors="coerce")
        orders["invoice_date"] = ts.dt.strftime(DIRTY_DATE_FMT)
        orders["quantity"] = orders.quantity.map(lambda q: f"{float(q)}")
        orders["channel"] = [rng.choice(["web", "store", "phone"]) for _ in range(len(orders))]
        for c in customers:
            c["loyalty_tier"] = rng.choice(["bronze", "silver", "gold"])
        truth["orders_date_format"] = DIRTY_DATE_FMT
        truth["new_columns"] = {"orders": ["channel"], "customers": ["loyalty_tier"]}

    # ---- day 2-3: schema drift; later days exercise data quality and corruption
    if day in (2, 3):
        orders = orders.rename(columns=ORDER_RENAMES)
        returns = returns.rename(columns=RETURN_RENAMES)
        truth["orders_renames"] = {v: k for k, v in ORDER_RENAMES.items()}     # source -> canonical
        truth["returns_renames"] = {v: k for k, v in RETURN_RENAMES.items()}

    # ---- write landing files
    (dst / "customers.json").write_text(json.dumps(customers, indent=1))
    returns.to_excel(dst / "returns.xlsx", index=False)

    if day == 5:
        cut = int(len(orders) * 0.7)
        orders.iloc[:cut].to_csv(dst / "orders_part1.csv", index=False)
        text = orders.iloc[cut:].to_csv(index=False)
        lines = text.split("\n")
        lines.insert(len(lines) // 2, ",".join(["\x00garbage\xff"] * 14))     # wrong field count + junk bytes
        broken = "\n".join(lines).rstrip("\n")
        last = broken.rsplit("\n", 1)[-1].split(",")
        broken = broken.rsplit("\n", 1)[0] + "\n" + ",".join(last[:5]) + ","  # missing price and trailing fields
        (dst / "orders_part2.csv").write_bytes(broken.encode("latin-1", errors="replace"))
        truth["corruption"] = {"files": ["orders_part2.csv"], "garbage_lines": 1, "truncated_rows": 1,
                               "note": "part2 is latin-1 with a junk line and a truncated final row"}
    else:
        orders.to_csv(dst / "orders.csv", index=False)

    truth_dir.mkdir(parents=True, exist_ok=True)
    (truth_dir / f"day_{day:02d}.json").write_text(json.dumps(truth, indent=1))
    return truth


def main() -> None:
    for d in range(1, N_DAYS + 1):
        t = apply_day(d, CLEAN_DIR, LANDING_DIR, TRUTH_DIR)
        summary = {k: (len(v) if isinstance(v, list) else v) for k, v in t["defects"].items()}
        print(f"day {d}: renames={len(t['orders_renames'])} new_cols={t['new_columns']} defects={summary} corruption={bool(t['corruption'])}")


if __name__ == "__main__":
    main()
