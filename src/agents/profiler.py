from datetime import datetime
from typing import Any

from src.agents.state import ColumnProfile

DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y",
)


def _as_text(value: Any) -> str:
    return str(value)[:120]


def _infer(values: list[Any]) -> tuple[str, list[str]]:
    non_null = [value for value in values if value not in (None, "")]
    if not non_null:
        return "null", []
    text_values = [_as_text(value) for value in non_null]
    integer = all(_is_number(value, integer=True) for value in text_values)
    number = all(_is_number(value, integer=False) for value in text_values)
    if integer:
        inferred = "integer"
    elif number:
        inferred = "number"
    else:
        inferred = "string"
    formats = []
    if inferred == "string":
        for date_format in DATE_FORMATS:
            if sum(_parses(value, date_format) for value in text_values) >= max(1, len(text_values) // 2):
                formats.append(date_format)
        if formats:
            inferred = "datetime"
    return inferred, formats


def _is_number(value: str, integer: bool) -> bool:
    try:
        parsed = float(value)
        return parsed.is_integer() if integer else True
    except (TypeError, ValueError):
        return False


def _parses(value: str, date_format: str) -> bool:
    try:
        datetime.strptime(value, date_format)
        return True
    except (TypeError, ValueError):
        return False


def profile_records(records: list[dict[str, Any]]) -> list[ColumnProfile]:
    columns = sorted({key for record in records for key in record})
    profiles = []
    row_count = len(records)
    for column in columns:
        values = [record.get(column) for record in records]
        null_count = sum(value in (None, "") for value in values)
        inferred_type, date_formats = _infer(values)
        profiles.append(ColumnProfile(
            name=column,
            inferred_type=inferred_type,
            row_count=row_count,
            null_count=null_count,
            null_rate=null_count / row_count if row_count else 0,
            examples=[_as_text(value) for value in values if value not in (None, "")][:5],
            date_formats=date_formats,
        ))
    return profiles
