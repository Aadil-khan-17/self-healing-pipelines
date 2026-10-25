import json
from pathlib import Path

from src.agents.state import RunSummary


def write_run_summary(summary: RunSummary, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{summary.batch_id}.json"
    path.write_text(json.dumps(summary.model_dump(), indent=2))
    return path
