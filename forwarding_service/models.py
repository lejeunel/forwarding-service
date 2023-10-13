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
    last_state: JobStatus = Field(sa_column=Column(Enum(JobStatus)), default=JobStatus.INIT)
    error: JobError = Field(sa_column=Column(Enum(JobError)))

    info: Dict[Any, Any] = Field(index=False, sa_column=Column(JSON))
    source: FileUrl
    destination: AnyUrl
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    regexp: str
    items: List["Item"] = Relationship(
        sa_relationship_kwargs={"cascade": "delete"}, back_populates="job"
    )

    def to_detailed_dict(self):
        result = dict(self)
        n_items = len(self.items)
        done_items = sum([item.status == ItemStatus.TRANSFERRED
                                        for item in self.items])
        result['total_num_items'] = n_items
        result['num_done_items'] = done_items
        if n_items > 0:

            result['progress'] = "{}%".format((done_items / n_items) * 100)
        else:
            result['progress'] = "nan"

        return result

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
