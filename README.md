# Self-Healing Ingestion Pipeline

An end-to-end data engineering and LLM-agent demo. It generates messy multi-source
data, runs a deterministic DuckDB baseline that fails in known ways, then uses
validated Groq decisions to repair schema and date drift. Every run can be scored
against generated ground truth and inspected in a Streamlit dashboard.

## Quick start

```bash
pip install -r requirements.txt
python -m scripts.make_clean_data      # 5 days of clean feeds  -> data/clean
python -m scripts.chaos                # break them             -> data/landing + data/ground_truth
pytest -q                              # sanity-check the chaos vs its own ground truth
python -m src.baseline --reset --all   # run the baseline pipeline over all 5 days
python -m scripts.evaluate             # score the generated runs
# optional dashboard: streamlit run scripts/dashboard.py
python -m scripts.agent_run --reset --all  # Groq-assisted run with audit trail
```

The baseline commands do not require an API key. The agent-assisted command does:

```powershell
$env:GROQ_API_KEY = "your-key"
.venv\Scripts\python.exe -m scripts.agent_run --reset --all
```

The current Groq model is `openai/gpt-oss-20b`. Never commit or paste the key into
the repository.

## What the baseline should do (expected results)

| Day | What changed | Baseline behaviour | Which agent fixes it |
|---|---|---|---|
| 1 | clean | SUCCESS | none |
| 2 | `quantity`→`qty`, `unit_price`→`price` (+2 more renames) | FAILED: unmapped required columns | Schema-Mapping |
| 3 | new columns, `DD/MM/YYYY HH:MM` dates, `"3.0"` quantities | FAILED: 100% rows have bad dates | Profiler + Transform |
| 4 | 5% null price, 2% negative qty, 5% dups, SCD2 city changes | SUCCESS with 26 quarantined, dups removed | none (validates DQ) |
| 5 | corrupted file: junk line + truncated row | SUCCESS with 1 line skipped + 1 row quarantined | Repair / Documentation |

Day 3 can be rescued by hand: `python -m src.baseline --day 3 --date-format "%d/%m/%Y %H:%M"`. That human override is what the Transform agent automates.

## Architecture

```
scripts/make_clean_data.py   deterministic synthetic orders/customers/returns feeds
scripts/chaos.py             controlled drift, defects, corruption, and ground truth
scripts/agent_run.py         Groq-assisted mapping/transform run with audit trail
scripts/evaluate.py          rules-only versus agent-assisted metrics
scripts/dashboard.py         Streamlit run history, DQ, decisions, approvals, repairs
src/ingest.py                robust readers -> schemaless bronze JSON with lineage
src/baseline.py              DuckDB bronze -> staging -> DQ -> quarantine -> silver -> gold
src/agents/state.py          Pydantic contracts for every structured agent response
src/agents/profiler.py       deterministic type, null-rate, and date-format profiling
src/agents/mapping.py        alias-first schema mapping with confidence
src/agents/transform.py      declarative numeric/date transforms and dry runs
src/agents/repair.py         bounded retry planning and escalation
src/agents/persistence.py    agent decisions, approval queue, and repair audit tables
src/agents/langgraph_workflow.py optional LangGraph-compatible graph entrypoint
test_data_layer.py           six fixture/ingestion tests
test_agents.py               approval, persistence, and repair tests
```

## Agent Flow

```text
profile -> map -> approval if confidence < 0.8 -> transform
	-> baseline run -> repair loop on failure -> document and audit
```

The profiler and baseline remain deterministic. The LLM only interprets profiles,
reviews mappings, and proposes declarative configuration. It never emits executable
Python or SQL. Pydantic validates every structured response before it is used.

The live runner persists decisions in `agent_decisions`, low-confidence proposals in
`approval_queue`, and repair attempts in `repair_attempts`. The dashboard provides
approve/reject controls for pending mappings.

## Implemented Components

1. Pydantic models for profiles, mappings, reviews, transforms, repairs, and summaries
2. Pure-Python profiling for types, null rates, examples, and date formats
3. Alias-first mapping with Groq review and confidence-based approval
4. Declarative transform configuration with profiler guardrails and dry-run support
5. Three-attempt repair planning followed by escalation
6. Optional MLflow spans around mapping and transform calls
7. Optional LangGraph graph compilation path
8. Comparative evaluation and an audit-focused Streamlit dashboard

Set `GROQ_API_KEY` to enable the optional LLM client in `src/agents/llm_client.py`.

## Results from the local run

The verified local run produced:

| Metric | Rules-only | Agent-assisted |
|---|---:|---:|
| Batch success rate | 60% | 100% |
| Mapping accuracy | - | 100% |
| Drift detection rate | - | 100% |
| Defect catch rate | - | 100% |
| Persisted agent decisions | 0 | 19 |
| Average batch duration | - | ~0.15 seconds |

Additional checks: day 4 quarantined 26 rows, day 5 quarantined 1 truncated row
after skipping 1 junk line, and 15 customers produced historical SCD2 versions.
The approval queue was empty in the successful run because all generated mappings
were above the 0.8 confidence threshold. Approval and escalation behavior is
covered by automated tests.

Run the dashboard with:

```bash
python -m streamlit run scripts/dashboard.py
```

Run the complete test suite with `pytest -q`; the verified suite contains 9 tests.

## Known Limitations

- The LangGraph module is an optional adapter; the production runner currently uses
	the same graph stages in ordinary Python for simpler local execution.
- MLflow spans are emitted when MLflow is installed and configured, but no hosted
	tracking server is required or bundled.
- The generated benchmark is intentionally small. Production use would need secret
	management, rate-limit handling, prompt/version tracking, and larger evaluations.
- Gold tables other than `dim_customer` are rebuilt each run; incremental MERGE is
	still a stretch goal.

## Databricks mapping

| Here | Databricks |
|---|---|
| `ingest.py` schemaless bronze | Auto Loader (`cloudFiles`) with schema evolution / `_rescued_data` |
| `run_dq` + quarantine | Lakeflow/DLT `EXPECT ... ON VIOLATION` + a quarantine table |
| `INSERT OR IGNORE`, SCD2 SQL | `MERGE INTO` (or `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`) |
| `pipeline_runs`, `dq_results` | Delta tables + Workflows job runs |
| DuckDB file | Delta tables in Unity Catalog |
