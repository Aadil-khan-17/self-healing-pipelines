from typing import Any, TypedDict


class PipelineState(TypedDict, total=False):
    batch_id: str
    records: list[dict[str, Any]]
    profiles: list[dict[str, Any]]
    mapping: dict[str, str]
    transform: dict[str, Any]
    status: str
    notes: list[str]


def _profile(state: PipelineState) -> PipelineState:
    from src.agents.profiler import profile_records
    state["profiles"] = [profile.model_dump() for profile in profile_records(state["records"])]
    return state


def _map(state: PipelineState) -> PipelineState:
    from src.agents.mapping import propose_mapping
    columns = [profile["name"] for profile in state["profiles"]]
    proposal = propose_mapping("orders", columns, ["invoice_no", "quantity", "invoice_date", "unit_price"])
    state["mapping"] = {item.canonical_col: item.source_col for item in proposal.mappings}
    return state


def _transform(state: PipelineState) -> PipelineState:
    from src.agents.transform import infer_transform_config
    state["transform"] = infer_transform_config(state["profiles"]).model_dump()
    return state


def _document(state: PipelineState) -> PipelineState:
    state.setdefault("notes", []).append("workflow documented")
    return state


def build_graph():
    try:
        from langgraph.graph import END, StateGraph
    except ImportError as exc:
        raise RuntimeError("Install langgraph to build the optional graph workflow") from exc

    graph = StateGraph(PipelineState)
    graph.add_node("profile", _profile)
    graph.add_node("map", _map)
    graph.add_node("transform", _transform)
    graph.add_node("document", _document)
    graph.set_entry_point("profile")
    graph.add_edge("profile", "map")
    graph.add_edge("map", "transform")
    graph.add_edge("transform", "document")
    graph.add_edge("document", END)
    return graph.compile()
