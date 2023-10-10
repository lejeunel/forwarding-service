import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic.networks import AnyUrl, FileUrl
from sqlalchemy import JSON
from sqlmodel import Column, Enum, Field, Relationship, SQLModel

from .enum_types import ItemStatus, JobError, JobStatus


class Job(SQLModel, table=True):
    id: Optional[UUID] = Field(
        default_factory=uuid.uuid4, primary_key=True, index=True, nullable=False
    )
    last_state: JobStatus = Field(sa_column=Column(Enum(JobStatus)))
    error: JobError = Field(sa_column=Column(Enum(JobError)))

    info: Dict[Any, Any] = Field(index=False, sa_column=Column(JSON))
    source: FileUrl
    destination: AnyUrl
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    regexp: str
    items: List["Item"] = Relationship(
        sa_relationship_kwargs={"cascade": "delete"}, back_populates="job"
    )


class Item(SQLModel, table=True):
    id: Optional[UUID] = Field(
        default_factory=uuid.uuid4, primary_key=True, index=True, nullable=False
    )
    in_uri: str
    out_uri: str

    status: ItemStatus = Field(
        default=ItemStatus.PENDING, sa_column=Column(Enum(ItemStatus))
    )
    job_id: UUID = Field(default=None, foreign_key="job.id")
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    transferred_at: Optional[datetime] = Field(default_factory=datetime.now)
    job: Optional[Job] = Relationship(back_populates="items")
