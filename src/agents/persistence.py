from datetime import datetime


DDL = """
CREATE TABLE IF NOT EXISTS agent_decisions (
    decision_id BIGINT,
    batch_id VARCHAR,
    agent VARCHAR,
    decision_type VARCHAR,
    input_summary VARCHAR,
    output_json VARCHAR,
    confidence DOUBLE,
    created_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS approval_queue (
    approval_id BIGINT,
    batch_id VARCHAR,
    feed VARCHAR,
    decision_json VARCHAR,
    confidence DOUBLE,
    status VARCHAR,
    reviewer VARCHAR,
    reviewed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS repair_attempts (
    attempt_id BIGINT,
    batch_id VARCHAR,
    attempt INTEGER,
    action VARCHAR,
    reason VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP
);
"""


def ensure_tables(con) -> None:
    con.execute(DDL)


def record_decision(con, batch_id: str, agent: str, decision_type: str,
                    input_summary: str, output_json: str, confidence: float | None = None) -> None:
    decision_id = con.execute("select coalesce(max(decision_id), 0) + 1 from agent_decisions").fetchone()[0]
    con.execute(
        "insert into agent_decisions values (?, ?, ?, ?, ?, ?, ?, ?)",
        [decision_id, batch_id, agent, decision_type, input_summary, output_json, confidence, datetime.now()],
    )


def queue_approval(con, batch_id: str, feed: str, decision_json: str, confidence: float) -> None:
    approval_id = con.execute("select coalesce(max(approval_id), 0) + 1 from approval_queue").fetchone()[0]
    con.execute(
        "insert into approval_queue values (?, ?, ?, ?, ?, 'PENDING', NULL, NULL)",
        [approval_id, batch_id, feed, decision_json, confidence],
    )


def record_repair(con, batch_id: str, attempt: int, action: str, reason: str, status: str) -> None:
    attempt_id = con.execute("select coalesce(max(attempt_id), 0) + 1 from repair_attempts").fetchone()[0]
    con.execute(
        "insert into repair_attempts values (?, ?, ?, ?, ?, ?, ?)",
        [attempt_id, batch_id, attempt, action, reason, status, datetime.now()],
    )
