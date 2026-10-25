"""Shared paths and the target-schema contract.

The ALIASES table is the *hand-written* column mapping the baseline pipeline uses.
In Phase 2 the Schema-Mapping Agent replaces it: when a source column is not in
this table, the baseline fails, and the agent proposes a mapping instead.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CLEAN_DIR = DATA / "clean"          # ground-truth "perfect" feeds
LANDING_DIR = DATA / "landing"      # what the pipeline actually sees (after chaos)
TRUTH_DIR = DATA / "ground_truth"   # what chaos broke, for evaluation
DB_PATH = DATA / "warehouse.duckdb"

N_DAYS = 5
ISO_FMT = "%Y-%m-%d %H:%M:%S"

# canonical column -> accepted source column names (nested JSON uses dotted paths)
ALIASES = {
    "orders": {
        "invoice_no":   ["invoice_no", "InvoiceNo", "Invoice"],
        "stock_code":   ["stock_code", "StockCode"],
        "description":  ["description", "Description"],
        "quantity":     ["quantity", "Quantity"],
        "invoice_date": ["invoice_date", "InvoiceDate"],
        "unit_price":   ["unit_price", "UnitPrice"],
        "customer_id":  ["customer_id", "CustomerID"],
        "country":      ["country", "Country"],
    },
    "customers": {
        "customer_id": ["customer_id"],
        "name":        ["name"],
        "email":       ["email"],
        "city":        ["address.city", "city"],
        "country":     ["address.country", "country"],
        "segment":     ["segment"],
        "updated_at":  ["updated_at"],
    },
    "returns": {
        "return_id":   ["return_id"],
        "invoice_no":  ["invoice_no"],
        "stock_code":  ["stock_code"],
        "quantity":    ["quantity"],
        "return_date": ["return_date"],
        "reason":      ["reason"],
    },
}

REQUIRED = {
    "orders": ["invoice_no", "stock_code", "quantity", "invoice_date", "unit_price"],
    "customers": ["customer_id", "updated_at"],
    "returns": ["return_id", "quantity", "return_date"],
}
