import duckdb

from src.agents.persistence import ensure_tables, queue_approval, record_repair
from src.agents.repair import plan_repair
from src.agents.state import MappingItem, MappingProposal


def test_low_confidence_mapping_requires_approval():
    proposal = MappingProposal(
        feed="orders",
        mappings=[MappingItem(source_col="amount", canonical_col="unit_price", confidence=0.65)],
    )
    assert proposal.requires_approval is True


def test_repair_escalates_after_three_attempts():
    plan = plan_repair("bad_invoice_date", 3)
    assert plan.action == "escalate"
    assert plan.max_attempts == 3


def test_audit_tables_store_approval_and_repair():
    con = duckdb.connect(":memory:")
    ensure_tables(con)
    queue_approval(con, "day_02", "orders", "{}", 0.65)
    record_repair(con, "day_03", 3, "escalate", "maximum repair attempts reached", "PLANNED")
    assert con.execute("select status from approval_queue").fetchone()[0] == "PENDING"
    assert con.execute("select action from repair_attempts").fetchone()[0] == "escalate"
