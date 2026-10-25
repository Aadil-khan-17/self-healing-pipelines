"""Generate 5 days of CLEAN source feeds (the ground truth) in the style of UCI Online Retail.

Three feeds per day:
  orders.csv      invoice lines (flat CSV)
  customers.json  API-style records with a nested `address` object (day 1 = full snapshot,
                  later days = incremental changes + new customers -> drives SCD2)
  returns.xlsx    returns from an Excel export

Realistic quirks are kept even in the "clean" data: cancellation invoices ('C' prefix,
negative quantity) and ~4% of invoices without a customer id.

Usage:  python -m scripts.make_clean_data
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.common import CLEAN_DIR, ISO_FMT, N_DAYS

CITIES = [("London", "United Kingdom"), ("Manchester", "United Kingdom"), ("Paris", "France"),
          ("Berlin", "Germany"), ("Madrid", "Spain"), ("Dublin", "Ireland"),
          ("Amsterdam", "Netherlands"), ("Lisbon", "Portugal")]
ADJ = ["Vintage", "Retro", "Hand-painted", "Ceramic", "Wooden", "Glass", "Pink", "Blue"]
ITEMS = ["Mug", "Lantern", "Candle Holder", "Tea Towel", "Cake Tin", "Bunting", "Tray", "Clock", "Jar", "Frame"]
REASONS = ["damaged", "wrong item", "changed mind", "late delivery"]


def generate(out_dir: Path = CLEAN_DIR, seed: int = 42, invoices_per_day: int = 120) -> None:
    rng = random.Random(seed)
    products = [(f"P{1000 + i}", f"{rng.choice(ADJ)} {rng.choice(ITEMS)} {i}", round(rng.uniform(0.5, 40), 2))
                for i in range(40)]
    start = datetime(2026, 1, 5)
    invoice_counter = 536000
    return_counter = 0
    customers: dict[int, dict] = {}

    def new_customer(cid: int, day: datetime) -> dict:
        city, country = rng.choice(CITIES)
        return {"customer_id": cid, "name": f"Customer {cid}", "email": f"c{cid}@example.com",
                "address": {"city": city, "country": country},
                "segment": rng.choice(["retail", "wholesale"]), "updated_at": day.strftime(ISO_FMT)}

    for d in range(1, N_DAYS + 1):
        day = start + timedelta(days=d - 1)
        changed: list[dict] = []

        # ---- customers: full snapshot on day 1, deltas afterwards
        if d == 1:
            for cid in range(10000, 10300):
                customers[cid] = new_customer(cid, day)
            changed = list(customers.values())
        else:
            for cid in range(10300 + (d - 2) * 3, 10300 + (d - 1) * 3):       # 3 new per day
                customers[cid] = new_customer(cid, day)
                changed.append(customers[cid])
            if d == 4:                                                          # 15 relocations -> SCD2
                for cid in rng.sample(sorted(customers)[:300], 15):
                    c = customers[cid]
                    city, country = rng.choice([
                        place for place in CITIES
                        if place != (c["address"]["city"], c["address"]["country"])
                    ])
                    c["address"] = {"city": city, "country": country}
                    c["updated_at"] = (day + timedelta(hours=9)).strftime(ISO_FMT)
                    changed.append(c)

        # ---- orders
        rows = []
        for _ in range(invoices_per_day):
            invoice_counter += 1
            is_cancel = rng.random() < 0.03
            has_customer = rng.random() > 0.04
            cid = rng.choice(sorted(customers)) if has_customer else None
            country = customers[cid]["address"]["country"] if cid else "Unspecified"
            ts = day + timedelta(hours=rng.randint(8, 18), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
            for code, desc, price in rng.sample(products, rng.randint(1, 5)):
                qty = rng.randint(1, 12)
                rows.append({"invoice_no": f"{'C' if is_cancel else ''}{invoice_counter}",
                             "stock_code": code, "description": desc,
                             "quantity": -qty if is_cancel else qty,
                             "invoice_date": ts.strftime(ISO_FMT), "unit_price": price,
                             "customer_id": cid if cid is not None else "", "country": country})
        orders = pd.DataFrame(rows)

        # ---- returns (sampled from today's non-cancelled orders)
        sold = orders[~orders.invoice_no.str.startswith("C")].sample(15, random_state=seed + d)
        rets = []
        for _, r in sold.iterrows():
            return_counter += 1
            rets.append({"return_id": f"R{return_counter:05d}", "invoice_no": r.invoice_no, "stock_code": r.stock_code,
                         "quantity": rng.randint(1, int(r.quantity)), "reason": rng.choice(REASONS),
                         "return_date": (datetime.strptime(r.invoice_date, ISO_FMT) + timedelta(days=1)).strftime(ISO_FMT)})
        returns = pd.DataFrame(rets)[["return_id", "invoice_no", "stock_code", "quantity", "return_date", "reason"]]

        # ---- write
        dd = out_dir / f"day_{d:02d}"
        dd.mkdir(parents=True, exist_ok=True)
        orders.to_csv(dd / "orders.csv", index=False)
        (dd / "customers.json").write_text(json.dumps(changed, indent=1))
        returns.to_excel(dd / "returns.xlsx", index=False)
        print(f"day {d}: {len(orders)} order lines, {len(changed)} customer records, {len(returns)} returns")


if __name__ == "__main__":
    generate()
