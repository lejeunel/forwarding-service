from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, validate_arguments
from sqlmodel import Session

from .enum_types import ItemStatus, JobError, JobStatus
from .models import Job
from .utils import check_field_exists, filter_table


class QueryArgs(BaseModel):
    id: UUID | None = None
    limit: int = 50
    sort_on: str | None = None
    source: str | None = None
    destination: str | None = None


class JobQueryArgs(QueryArgs):
    status: JobStatus | None = None
    error: JobError | None = None
    created_at: datetime | None = None
    sort_on: str = 'created_at'


class ItemQueryArgs(QueryArgs):
    status: ItemStatus | None = None
    job_id: UUID | None = None


class Query:
    def __init__(self, session: Session, model: BaseModel):
        self.session = session
        self.model = model

    @validate_arguments
    def get(self, query_args: QueryArgs | None = QueryArgs()):
        check_field_exists(self.model, query_args.sort_on)

        query = filter_table(
            self.session,
            self.model,
            **dict(query_args),
        )

        if query_args.sort_on is not None:
            field = getattr(Job, query_args.sort_on)
            query = query.order_by(field.desc())

        query = query.limit(query_args.limit)

        objects = query.all()[::-1]

        return objects

    @validate_arguments
    def exists(self, id: UUID):
        count = self.session.query(self.model).count()

        if count > 0:
            return True

        return False

    @validate_arguments
    def delete(self, query_args: QueryArgs | None = QueryArgs()):
        objects = self.get(query_args)
        if len(objects) == 0:
            raise Exception(
                f"Could not find {type(self.model)} with query arguments {dict(query_args)}"
            )

        for obj in objects:
            self.session.delete(obj)
            self.session.commit()
