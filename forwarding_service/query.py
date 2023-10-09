from typing import Optional
from uuid import UUID

from pydantic import BaseModel, validate_call

from .enum_types import ItemStatus, JobError, JobStatus
from .models import Item, Job
from .utils import check_enum, check_field_exists, filter_table


class JobQueryArgs(BaseModel):
    id: Optional[UUID] = None
    status: Optional[JobStatus] = None
    error: Optional[JobError] = None
    limit: int = 50
    sort_on: Optional[str] = None


class ItemQueryArgs(JobQueryArgs):
    source: Optional[str] = None
    destination: Optional[str] = None
    status: Optional[ItemStatus] = None
    job_id: Optional[UUID] = None


class Query:
    def __init__(self, session):
        self.session = session

    @validate_call
    def job_exists(self, job_id: UUID):
        return self.session.query(Job).filter(Job.id == job_id).count() > 0

    def jobs(self, query_args: JobQueryArgs):
        """Return Jobs after applying filters

        Args:
            id (str, optional): Job id filtering. Defaults to None.
            status (str, optional): status filtering. Defaults to None.
            limit (int, optional): limit number of Job. Defaults to 50.
            sort_on (str, optional): sort job by. Defaults to "created".

        Returns:
            [JobSchema]: return Job representation in a list
        """
        from .models import Job

        check_field_exists(Job, query_args.sort_on)
        check_enum(JobStatus, query_args.status)
        check_enum(JobError, query_args.error)

        query = filter_table(
            self.session,
            Job,
            **{
                "id": query_args.id,
                "last_state": query_args.status,
                "error": query_args.error,
                "limit": query_args.limit,
                "sort_on": query_args.sort_on,
            },
        )

        if query_args.sort_on is not None:
            field = getattr(Job, query_args.sort_on)
            query = query.order_by(field.desc())

        query = query.limit(query_args.limit)

        jobs = query.all()[::-1]

        # return [job.to_detailed_dict() for job in jobs]
        return jobs

    @validate_call
    def items(self, query_args: ItemQueryArgs):
        check_field_exists(Item, query_args.sort_on)
        check_enum(ItemStatus, query_args.status)

        query = filter_table(
            self.session,
            Item,
            **{
                "id": query_args.id,
                "job_id": query_args.job_id,
                "status": query_args.status,
                "limit": query_args.limit,
                "sort_on": query_args.sort_on,
                "in_uri": query_args.source,
                "out_uri": query_args.destination,
            },
        )
        if query_args.sort_on is not None:
            field = getattr(Item, query_args.sort_on)
            query = query.order_by(field)

        query = query.limit(query_args.limit)
        items = query.all()

        return items
