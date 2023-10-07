from uuid import UUID
from pydantic import validate_arguments
from typing import Optional

from .models import Job, Item
from .utils import check_field_exists, check_enum, filter_table
from .enum_types import ItemStatus, JobError, JobStatus


class Query:
    def __init__(self, session):
        self.session = session

    @validate_arguments
    def job_exists(self, job_id: UUID):
        return self.session.query(Job).filter(Job.id == job_id).count() > 0

    @validate_arguments
    def jobs(self, id=None, status=None, error=None, limit=50, sort_on=None):
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

        # return [job.to_detailed_dict() for job in jobs]
        return jobs

    @validate_arguments
    def items(
        self,
        id: Optional[UUID] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        status: Optional[JobStatus] = None,
        job_id: Optional[UUID] = None,
        limit: int = 50,
        sort_on: Optional[str] = None,
    ):
        check_field_exists(Item, sort_on)
        check_enum(ItemStatus, status)

        query = filter_table(
            self.session,
            Item,
            **{"job_id": job_id, "status": status, "limit": limit, "sort_on": sort_on},
        )
        if sort_on is not None:
            field = getattr(Item, sort_on)
            query = query.order_by(field)

        query = query.limit(limit)
        items = query.all()

        return items
