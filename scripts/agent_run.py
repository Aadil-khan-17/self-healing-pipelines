"""Run the baseline with Groq-assisted mapping and transform decisions."""
import argparse
import json

import duckdb

from src.agents.llm_client import LLMClient
from src.agents.mapping import propose_mapping
from src.agents.persistence import ensure_tables, queue_approval, record_decision, record_repair
from src.agents.profiler import profile_records
from src.agents.repair import plan_repair
from src.agents.state import MappingReview, TransformConfig
from src.agents.trace import log_event, trace_span
from src.baseline import DDL, run_batch
from src.common import ALIASES, DB_PATH, ISO_FMT, LANDING_DIR, N_DAYS, REQUIRED
from src.ingest import load_batch


def records_for(feed_batch) -> list[dict]:
    return [json.loads(raw) for raw in feed_batch.frame["raw"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=int)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    client = LLMClient()
    if args.reset and DB_PATH.exists():
        DB_PATH.unlink()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    ensure_tables(con)
    days = range(1, N_DAYS + 1) if args.all else [args.day]

    for day in days:
        batch_id = f"day_{day:02d}"
        feeds = load_batch(LANDING_DIR / batch_id, batch_id)
        overrides = {}
        for feed, feed_batch in feeds.items():
            profiles = profile_records(records_for(feed_batch))
            source_cols = feed_batch.columns
            proposal = propose_mapping(feed, source_cols, list(ALIASES[feed]))
            overrides[feed] = {
                item.canonical_col: item.source_col for item in proposal.mappings
            }
            proposal_json = proposal.model_dump_json()
            record_decision(con, batch_id, "schema_mapping", "proposal", feed, proposal_json,
                            min((item.confidence for item in proposal.mappings), default=0.0))
            low_confidence = [item for item in proposal.mappings if item.confidence < 0.8]
            if low_confidence:
                queue_approval(con, batch_id, feed, proposal_json, min(item.confidence for item in low_confidence))
            if day == 2:
                with trace_span("mapping_review", batch_id=batch_id, feed=feed) as span:
                    review = client.structured(
                        "Review the proposed source-to-canonical mapping. Return JSON only.",
                        json.dumps({
                            "feed": feed,
                            "source_columns": source_cols,
                            "proposed_mapping": overrides[feed],
                        }),
                        MappingReview,
                    )
                    log_event(span, {"valid": review.mapping_valid, "issue_count": len(review.issues)})
                record_decision(con, batch_id, "schema_mapping", "review", feed,
                                review.model_dump_json(), 1.0 if review.mapping_valid else 0.0)
                print(f"[{batch_id}] {feed} mapping review: valid={review.mapping_valid} issues={len(review.issues)}")

        date_fmt = ISO_FMT
        if day == 3:
            profiles = profile_records(records_for(feeds["orders"]))
            with trace_span("transform_interpretation", batch_id=batch_id) as span:
                transform = client.structured(
                    "Interpret this profile and return a TransformConfig JSON object. "
                    "Use the detected date format for invoice_date.",
                    json.dumps([profile.model_dump() for profile in profiles]),
                    TransformConfig,
                )
                log_event(span, {"date_formats": transform.date_formats})
            detected_formats = next(
                (profile.date_formats for profile in profiles if profile.name == "InvoiceDate"),
                [],
            )
            proposed_format = transform.date_formats.get("invoice_date") or transform.date_formats.get("InvoiceDate")
            date_fmt = proposed_format if proposed_format in detected_formats else (detected_formats[0] if detected_formats else ISO_FMT)
            record_decision(con, batch_id, "transform", "config", "orders profile",
                            transform.model_dump_json(), 1.0 if proposed_format in detected_formats else 0.5)
            print(f"[{batch_id}] transform date format: {date_fmt}")

        result = run_batch(con, day, date_fmt, overrides)
        attempt = 0
        while result["status"] == "FAILED" and attempt < 3:
            attempt += 1
            plan = plan_repair(" | ".join(result["notes"]), attempt)
            record_repair(con, batch_id, attempt, plan.action, plan.reason, "PLANNED")
            record_decision(con, batch_id, "repair", "plan", result["status"], plan.model_dump_json())
            if plan.action != "retry":
                break
            result = run_batch(con, day, plan.date_format or date_fmt, overrides)
            record_repair(con, batch_id, attempt, plan.action, plan.reason, result["status"])
        print(f"[{batch_id}] {result['status']} quarantined={result['quarantined']}")
        for note in result["notes"]:
            print(f"    - {note}")


if __name__ == "__main__":
    main()
