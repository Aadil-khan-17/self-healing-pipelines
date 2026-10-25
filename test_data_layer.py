"""The chaos script is your eval ground truth, so test that the truth file matches what actually landed."""
import json

import pandas as pd
import pytest

from scripts.chaos import apply_day
from scripts.make_clean_data import generate
from src.baseline import json_path, resolve_mapping
from src.ingest import load_batch


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    root = tmp_path_factory.mktemp("etl")
    clean, landing, truth = root / "clean", root / "landing", root / "truth"
    generate(clean)
    truths = {d: apply_day(d, clean, landing, truth) for d in range(1, 6)}
    return landing, truths


def batch(env, d):
    return load_batch(env[0] / f"day_{d:02d}", f"day_{d:02d}")


def test_day1_is_clean_and_maps_with_static_aliases(env):
    b = batch(env, 1)
    for feed, fb in b.items():
        _, missing, unmapped = resolve_mapping(feed, fb.columns)
        assert missing == [] and unmapped == []


def test_day2_renames_break_static_mapping(env):
    b = batch(env, 2)
    _, missing, _ = resolve_mapping("orders", b["orders"].columns)
    assert set(missing) == {"quantity", "unit_price"}          # qty / price are not in ALIASES
    assert set(env[1][2]["orders_renames"].values()) >= {"quantity", "unit_price"}


def test_day3_new_column_and_date_format(env):
    b = batch(env, 3)
    assert "channel" in b["orders"].columns
    first = json.loads(b["orders"].frame.raw.iloc[0])
    pd.to_datetime(first["InvoiceDate"], format="%d/%m/%Y %H:%M")   # parses in the dirty format
    with pytest.raises(ValueError):
        pd.to_datetime(first["InvoiceDate"], format="%Y-%m-%d %H:%M:%S")


def test_day4_defect_counts_match_truth(env):
    t = env[1][4]["defects"]
    rows = [json.loads(r) for r in batch(env, 4)["orders"].frame.raw]
    df = pd.DataFrame(rows)
    assert df.unit_price.isna().sum() == len(t["null_unit_price"])
    assert df.description.isna().sum() == len(t["null_description"])
    keys = df.invoice_no + "|" + df.stock_code
    assert keys.duplicated().sum() == len(t["duplicate_extra_rows"])
    assert (df.quantity.astype(float) < 0)[~df.invoice_no.str.startswith("C")].sum() == len(t["negative_qty_on_sale"])


def test_day5_corruption_is_survivable_and_counted(env):
    fb = batch(env, 5)["orders"]
    assert fb.rejected_lines == env[1][5]["corruption"]["garbage_lines"]
    assert set(fb.files) == {"orders_part1.csv", "orders_part2.csv"}


def test_nested_customer_paths(env):
    fb = batch(env, 1)["customers"]
    assert "address.city" in fb.columns
    assert json_path("address.city") == '$."address"."city"'
