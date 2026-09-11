from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Identity = Annotated[str, Field(min_length=1, max_length=128)]


class Rules(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    rollout_bps: int = Field(default=0, ge=0, le=10000)
    stickiness: Literal["user", "organization"] = "user"
    organizations: list[Identity] = Field(default_factory=list, max_length=100)
    groups: list[Identity] = Field(default_factory=list, max_length=100)
    excluded_users: list[Identity] = Field(default_factory=list, max_length=100)


class Flag(Rules):
    key: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_.-]*$")
    salt: UUID


class Context(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str = Field(min_length=1, max_length=128)
    organization_id: str | None = Field(default=None, max_length=128)
    groups: list[Identity] = Field(default_factory=list, max_length=32)


class Snapshot(BaseModel):
    environment_id: UUID
    revision: int = Field(ge=1)
    enabled: bool
    flags: list[Flag] = Field(max_length=100)


class Evaluation(BaseModel):
    value: bool
    reason: str
    revision: int | None = None
    bucket: int | None = None
    stale: bool = False
