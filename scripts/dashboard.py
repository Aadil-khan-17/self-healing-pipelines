from pathlib import Path

import duckdb
import streamlit as st

from src.common import DB_PATH


st.set_page_config(page_title="TBSC pipeline", layout="wide")
st.title("Self-Healing Ingestion Pipeline")


def _table_exists(connection, table: str) -> bool:
    return bool(connection.execute(
        "select count(*) from information_schema.tables where table_name = ?", [table]
    ).fetchone()[0])


if not Path(DB_PATH).exists():
    st.error("Run the baseline pipeline first.")
    st.stop()

con = duckdb.connect(str(DB_PATH), read_only=True)
runs = con.execute("select * from pipeline_runs order by started_at").fetchdf()
dq = con.execute("select * from dq_results order by batch_id, feed").fetchdf()
quarantine = con.execute("select * from quarantine_rows").fetchdf()
customers = con.execute("select * from dim_customer order by customer_id, valid_from").fetchdf()
decisions = con.execute("select * from agent_decisions order by created_at").fetchdf() if _table_exists(con, "agent_decisions") else None
approvals = con.execute("select * from approval_queue order by approval_id").fetchdf() if _table_exists(con, "approval_queue") else None
repairs = con.execute("select * from repair_attempts order by created_at").fetchdf() if _table_exists(con, "repair_attempts") else None

left, middle, right = st.columns(3)
left.metric("Runs", len(runs))
middle.metric("Successful", int((runs.status == "SUCCESS").sum()))
right.metric("Quarantined", int(runs.rows_quarantined.sum()))

st.subheader("Run history")
st.dataframe(runs, use_container_width=True)
st.subheader("Data-quality results")
st.dataframe(dq, use_container_width=True)
st.subheader("Approval and quarantine queue")
st.dataframe(quarantine, use_container_width=True)
st.subheader("Customer history")
st.dataframe(customers, use_container_width=True)
if decisions is not None:
    st.subheader("Agent decisions")
    st.dataframe(decisions, use_container_width=True)
if approvals is not None:
    st.subheader("Approval queue")
    st.dataframe(approvals, use_container_width=True)
    pending = approvals[approvals.status == "PENDING"]
    for approval_id in pending.approval_id:
        row = pending[pending.approval_id == approval_id].iloc[0]
        left, right = st.columns(2)
        left.write(f"{row.batch_id} / {row.feed} confidence={row.confidence:.2f}")
        if right.button("Approve", key=f"approve_{approval_id}"):
            con.execute(
                "update approval_queue set status = 'APPROVED', reviewer = 'dashboard', reviewed_at = current_timestamp where approval_id = ?",
                [int(approval_id)],
            )
            st.rerun()
        if right.button("Reject", key=f"reject_{approval_id}"):
            con.execute(
                "update approval_queue set status = 'REJECTED', reviewer = 'dashboard', reviewed_at = current_timestamp where approval_id = ?",
                [int(approval_id)],
            )
            st.rerun()
if repairs is not None:
    st.subheader("Repair attempts")
    st.dataframe(repairs, use_container_width=True)
