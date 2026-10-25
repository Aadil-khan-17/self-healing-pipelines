from dataclasses import dataclass, field
from typing import Any

from src.agents.documentation import write_run_summary
from src.agents.mapping import propose_mapping
from src.agents.profiler import profile_records
from src.agents.repair import plan_repair
from src.agents.state import RunSummary
from src.agents.trace import trace_span
from src.agents.transform import infer_transform_config


@dataclass
class WorkflowState:
    batch_id: str
    feed: str
    records: list[dict[str, Any]]
    canonical_columns: list[str]
    notes: str = ""
    attempts: int = 0
    decisions: list[str] = field(default_factory=list)


def run_agent_workflow(state: WorkflowState, output_dir=None) -> WorkflowState:
    with trace_span("profile", batch_id=state.batch_id, feed=state.feed):
        profiles = profile_records(state.records)
    state.decisions.append(f"profiled {len(profiles)} columns")

    with trace_span("map", batch_id=state.batch_id):
        proposal = propose_mapping(state.feed, [profile.name for profile in profiles], state.canonical_columns)
    state.decisions.append(f"mapped {len(proposal.mappings)} columns; approval={proposal.requires_approval}")

    with trace_span("transform", batch_id=state.batch_id):
        config = infer_transform_config([profile.model_dump() for profile in profiles])
    state.decisions.append(f"inferred {len(config.date_formats)} date formats")

    if state.notes:
        plan = plan_repair(state.notes, state.attempts)
        state.decisions.append(f"repair action={plan.action}")

    if output_dir is not None:
        write_run_summary(RunSummary(
            batch_id=state.batch_id,
            status="PROFILED",
            notes=[state.notes] if state.notes else [],
            agent_decisions=state.decisions,
        ), output_dir)
    return state
