from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, validate_arguments

from .enum_types import ItemStatus, JobError, JobStatus
from .models import Item, Job
from .utils import filter_table


class JobQueryArgs(BaseModel):
    id: Optional[UUID] = None
    last_state: Optional[JobStatus] = None
    error: Optional[JobError] = None
    limit: int = 50
    sort_on: Optional[str] = None
    source: Optional[str] = None
    destination: Optional[str] = None
    created_at: Optional[datetime] = None

class ItemQueryArgs(BaseModel):
    id: Optional[UUID] = None
    source: Optional[str] = None
    destination: Optional[str] = None
    status: Optional[ItemStatus] = None
    job_id: Optional[UUID] = None
    limit: int = 50
    sort_on: Optional[str] = None

class Query:
    def __init__(self, session):
        self.session = session

    @validate_arguments
    def job_exists(self, job_id: UUID):
        return self.session.query(Job).filter(Job.id == job_id).count() > 0

    def jobs(
        self,
            query_args: Optional[JobQueryArgs] = JobQueryArgs()
    ):
        """Return Jobs after applying filters"""
        from .models import Job

        query = filter_table(
            self.session,
            Job,
            **dict(query_args),
        )

        if query_args.sort_on is not None:
            field = getattr(Job, query_args.sort_on)
            query = query.order_by(field.desc())

        query = query.limit(query_args.limit)

        jobs = query.all()[::-1]

        return jobs

    @validate_arguments
    def items(
        self,
            query_args: Optional[ItemQueryArgs] = ItemQueryArgs()
    ):

        query = filter_table(
            self.session,
            Item,
            **dict(query_args),
        )
        if query_args.sort_on is not None:
            field = getattr(Item, query_args.sort_on)
            query = query.order_by(field)

        query = query.limit(query_args.limit)
        items = query.all()

        return items
