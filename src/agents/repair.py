from src.agents.state import RepairPlan


def plan_repair(notes: str, attempt: int) -> RepairPlan:
    if attempt >= 3:
        return RepairPlan(action="escalate", reason="maximum repair attempts reached")
    if "bad_invoice_date" in notes or "date" in notes.lower():
        return RepairPlan(
            action="retry",
            reason="retry with the detected non-ISO date format",
            date_format="%d/%m/%Y %H:%M",
        )
    return RepairPlan(action="escalate", reason="no deterministic repair matched the failure")
