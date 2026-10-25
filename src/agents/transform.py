from typing import Any

from src.agents.state import TransformConfig


def infer_transform_config(profiles: list[dict[str, Any]]) -> TransformConfig:
    config = TransformConfig()
    for profile in profiles:
        name = profile["name"]
        if profile["inferred_type"] in {"integer", "number"}:
            config.numeric_casts[name] = profile["inferred_type"]
        if profile.get("date_formats"):
            config.date_formats[name] = profile["date_formats"][0]
    return config


def dry_run(records: list[dict[str, Any]], config: TransformConfig) -> list[dict[str, Any]]:
    result = []
    for record in records:
        transformed = dict(record)
        for column, cast in config.numeric_casts.items():
            if transformed.get(column) in (None, ""):
                continue
            value = float(transformed[column])
            transformed[column] = int(value) if cast == "integer" else value
        result.append(transformed)
    return result
