import uuid

import sqlalchemy as sa
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy_utils.types.uuid import UUIDType

from app.enum_types import ItemStatus, JobStatus, JobError

from . import db


class Item(db.Model):
    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    uri = db.Column(db.String)
    status = db.Column(db.Enum(ItemStatus))
    job_id = db.Column(UUIDType, sa.ForeignKey("job.id"))
    creation_date = db.Column(db.DateTime(
        timezone=True), server_default=func.now())
    upload_date = db.Column(db.DateTime(timezone=True), nullable=True)


class Job(db.Model):

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    last_state = db.Column(db.Enum(JobStatus), default=JobStatus.INITIATED)
    error = db.Column(db.Enum(JobError), default=JobError.NONE)
    info = db.Column(db.JSON)
    user = db.Column(db.String)
    source = db.Column(db.String)
    destination = db.Column(db.String)
    regexp = db.Column(db.String)
    creation_date = db.Column(db.DateTime(
        timezone=True), server_default=func.now())
