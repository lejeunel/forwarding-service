from uuid import UUID

from pydantic import validate_call

from .enum_types import ItemStatus, JobError, JobStatus
from .models import Item, Job
from .utils import check_enum, check_field_exists, filter_table


class Query:
    def __init__(self, session):
        self.session = session

    @validate_call
    def job_exists(self, job_id: UUID):
        return self.session.query(Job).filter(Job.id == job_id).count() > 0

    @validate_call
    def jobs(
        self,
        id: UUID | None = None,
        status: JobStatus | None = None,
        error: JobError | None = None,
        limit: int = 50,
        sort_on: str | None = None,
    ):
        """Return Jobs after applying filters"""
        from .models import Job

        check_field_exists(Job, sort_on)
        check_enum(JobStatus, status)
        check_enum(JobError, error)

        query = filter_table(
            self.session,
            Job,
            **{
                "id": id,
                "last_state": status,
                "error": error,
                "limit": limit,
                "sort_on": sort_on,
            },
        )

        if sort_on is not None:
            field = getattr(Job, sort_on)
            query = query.order_by(field.desc())

        query = query.limit(limit)

        jobs = query.all()[::-1]

        return jobs

    @validate_call
    def items(
        self,
        id: UUID | None = None,
        job_id: UUID | None = None,
        source: str | None = None,
        destination: str | None = None,
        status: ItemStatus | None = None,
        limit: int = 50,
        sort_on: str | None = None,
    ):
        check_field_exists(Item, sort_on)
        check_enum(ItemStatus, status)

        query = filter_table(
            self.session,
            Item,
            **{
                "id": id,
                "job_id": job_id,
                "status": status,
                "limit": limit,
                "sort_on": sort_on,
                "in_uri": source,
                "out_uri": destination,
            },
        )
        if sort_on is not None:
            field = getattr(Item, sort_on)
            query = query.order_by(field)

        query = query.limit(limit)
        items = query.all()

        return items
