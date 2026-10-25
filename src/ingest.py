"""Bronze ingestion: read whatever landed, keep every row as a raw JSON document + lineage metadata.

Why schemaless bronze? Each row is stored as one JSON string, so a renamed or new column
can never break ingestion. Drift only shows up in the *mapping* step (bronze -> silver),
which is exactly where the agents will plug in. (Databricks equivalent: Auto Loader with
schema evolution / `_rescued_data`.)
"""
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

FEED_PATTERNS = {"orders": "orders*.csv", "customers": "customers*.json", "returns": "returns*.xlsx"}


@dataclass
class FeedBatch:
    feed: str
    batch_id: str
    frame: pd.DataFrame                      # _batch_id, _source_file, _ingest_ts, _row_num, raw
    columns: list[str]                       # union of (flattened) source columns seen
    files: list[str] = field(default_factory=list)
    rejected_lines: int = 0                  # lines the parser could not turn into a row


def _none_if_na(v):
    return None if pd.isna(v) else v


def _flatten(rec: dict, prefix: str = "") -> dict:
    out = {}
    for k, v in rec.items():
        if isinstance(v, dict):
            out.update(_flatten(v, f"{prefix}{k}."))
        else:
            out[f"{prefix}{k}"] = v
    return out


def _read_csv(path: Path) -> tuple[list[dict], list[str], int]:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""],
                     on_bad_lines="skip", encoding_errors="replace")
    n_lines = len(path.read_bytes().splitlines()) - 1          # minus header
    recs = [{k: _none_if_na(v) for k, v in r.items()} for r in df.to_dict("records")]
    return recs, list(df.columns), max(0, n_lines - len(df))


def _read_xlsx(path: Path) -> tuple[list[dict], list[str], int]:
    df = pd.read_excel(path, dtype=str)
    recs = [{k: _none_if_na(v) for k, v in r.items()} for r in df.to_dict("records")]
    return recs, list(df.columns), 0


def _read_json(path: Path) -> tuple[list[dict], list[str], int]:
    data = json.loads(path.read_text())
    recs = data if isinstance(data, list) else [data]
    cols: dict[str, None] = {}
    for r in recs:
        cols.update(dict.fromkeys(_flatten(r)))
    return recs, list(cols), 0


def load_batch(day_dir: Path, batch_id: str) -> dict[str, FeedBatch]:
    """Read every feed present in a landing folder."""
    readers = {"orders": _read_csv, "customers": _read_json, "returns": _read_xlsx}
    now, batches = datetime.now(), {}
    for feed, pattern in FEED_PATTERNS.items():
        files = sorted(day_dir.glob(pattern))
        if not files:
            continue
        rows, cols, rejected = [], {}, 0
        for f in files:
            recs, fcols, rej = readers[feed](f)
            rejected += rej
            cols.update(dict.fromkeys(fcols))
            for i, rec in enumerate(recs):
                rows.append({"_batch_id": batch_id, "_source_file": f.name, "_ingest_ts": now,
                             "_row_num": i, "raw": json.dumps(rec)})
        frame = pd.DataFrame(rows, columns=["_batch_id", "_source_file", "_ingest_ts", "_row_num", "raw"])
        batches[feed] = FeedBatch(feed, batch_id, frame, list(cols), [f.name for f in files], rejected)
    return batches
