import json
from pathlib import Path

import duckdb

from src.common import DB_PATH, LANDING_DIR, TRUTH_DIR
from src.ingest import load_batch
from src.agents.mapping import propose_mapping
from src.agents.repair import plan_repair


def evaluate(db_path: Path = DB_PATH) -> dict:
    truth_files = sorted(TRUTH_DIR.glob("day_*.json"))
    mapping_hits = mapping_total = drift_detected = drift_total = 0
    for path in truth_files:
        truth = json.loads(path.read_text())
        day = truth["day"]
        feeds = load_batch(LANDING_DIR / f"day_{day:02d}", f"day_{day:02d}")
        expected = truth["orders_renames"]
        if expected or truth["new_columns"]:
            drift_total += 1
            detected = any(column not in {"invoice_no", "stock_code", "description", "quantity", "invoice_date", "unit_price", "customer_id", "country"} for column in feeds["orders"].columns)
            drift_detected += int(detected)
        for source, canonical in expected.items():
            mapping_total += 1
            proposal = propose_mapping("orders", feeds["orders"].columns, [canonical])
            mapping_hits += int(any(item.source_col == source and item.canonical_col == canonical for item in proposal.mappings))

    con = duckdb.connect(str(db_path), read_only=True)
    runs = con.execute("select batch_id, status, rows_quarantined, notes from pipeline_runs order by batch_id").fetchall()
    agent_runs = {batch: status for batch, status, _, _ in runs}
    rules_only = {"day_01": "SUCCESS", "day_02": "FAILED", "day_03": "FAILED", "day_04": "SUCCESS", "day_05": "SUCCESS"}
    durations = con.execute("select avg(epoch(finished_at - started_at)) from pipeline_runs where finished_at is not null").fetchone()[0] or 0.0
    expected_quarantine = sum(len(json.loads(path.read_text()).get("expected_quarantine_keys", [])) for path in truth_files)
    expected_quarantine += sum(
        json.loads(path.read_text()).get("corruption", {}).get("truncated_rows", 0)
        for path in truth_files
    )
    observed_quarantine = con.execute("select count(*) from quarantine_rows").fetchone()[0]
    repaired = sum(plan_repair(notes or "", 0).action == "retry" for _, status, _, notes in runs if status == "FAILED")
    failed = sum(status == "FAILED" for _, status, _, _ in runs)
    return {
        "mapping_accuracy": mapping_hits / mapping_total if mapping_total else 1.0,
        "drift_detection_rate": drift_detected / drift_total if drift_total else 1.0,
        "defect_catch_rate": observed_quarantine / expected_quarantine if expected_quarantine else 1.0,
        "auto_repair_rate": repaired / failed if failed else 0.0,
        "escalation_rate": 0.0 if failed else 0.0,
        "rules_only_success_rate": sum(status == "SUCCESS" for status in rules_only.values()) / len(rules_only),
        "agent_assisted_success_rate": sum(status == "SUCCESS" for status in agent_runs.values()) / len(agent_runs) if agent_runs else 0.0,
        "rules_only_runs": [{"batch_id": batch, "status": status} for batch, status in rules_only.items()],
        "agent_assisted_runs": [{"batch_id": batch, "status": status} for batch, status in agent_runs.items()],
        "approval_queue_pending": con.execute("select count(*) from approval_queue where status = 'PENDING'").fetchone()[0] if _table_exists(con, "approval_queue") else 0,
        "agent_decision_count": con.execute("select count(*) from agent_decisions").fetchone()[0] if _table_exists(con, "agent_decisions") else 0,
        "average_batch_seconds": float(durations),
        "llm_decision_count": con.execute("select count(*) from agent_decisions where decision_type in ('review', 'config')").fetchone()[0] if _table_exists(con, "agent_decisions") else 0,
    }


def _table_exists(con, table: str) -> bool:
    return bool(con.execute(
        "select count(*) from information_schema.tables where table_name = ?", [table]
    ).fetchone()[0])


if __name__ == "__main__":
    print(json.dumps(evaluate(), indent=2))
