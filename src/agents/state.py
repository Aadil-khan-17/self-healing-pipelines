from typing import Literal

from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    name: str
    inferred_type: Literal["integer", "number", "string", "datetime", "boolean", "null", "mixed"]
    row_count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    null_rate: float = Field(ge=0, le=1)
    examples: list[str] = Field(default_factory=list, max_length=5)
    date_formats: list[str] = Field(default_factory=list)


class MappingItem(BaseModel):
    source_col: str
    canonical_col: str
    confidence: float = Field(ge=0, le=1)
    reason: str = ""


class MappingProposal(BaseModel):
    feed: str
    mappings: list[MappingItem] = Field(default_factory=list)
    unmapped_source_cols: list[str] = Field(default_factory=list)
    requires_approval: bool = False

    def model_post_init(self, __context: object) -> None:
        self.requires_approval = any(item.confidence < 0.8 for item in self.mappings)


class MappingReview(BaseModel):
    mapping_valid: bool | None = None
    status: str | None = None
    issues: list[str] = Field(default_factory=list)

    def model_post_init(self, __context: object) -> None:
        if self.mapping_valid is None and self.status is not None:
            self.mapping_valid = self.status.lower() in {"valid", "approved", "ok"}


class TransformConfig(BaseModel):
    date_formats: dict[str, str] = Field(default_factory=dict)
    numeric_casts: dict[str, Literal["integer", "number"]] = Field(default_factory=dict)
    drop_columns: list[str] = Field(default_factory=list)


class RepairPlan(BaseModel):
    action: Literal["retry", "escalate", "skip"]
    reason: str
    date_format: str | None = None
    max_attempts: int = Field(default=3, ge=1, le=3)


class RunSummary(BaseModel):
    batch_id: str
    status: str
    notes: list[str] = Field(default_factory=list)
    agent_decisions: list[str] = Field(default_factory=list)
