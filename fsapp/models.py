import uuid

import sqlalchemy as sa
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.sql import func
from sqlalchemy_utils.types.uuid import UUIDType

from fsapp.enum_types import FileStatus, JobStatus, JobError

from . import db


class File(db.Model):
    __tablename__ = "file"

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    filename = db.Column(db.String)
    status = db.Column(db.Enum(FileStatus))
    job_id = db.Column(UUIDType, sa.ForeignKey("jobs.id"))
    creation_date = db.Column(db.DateTime(
        timezone=True), server_default=func.now())
    upload_date = db.Column(db.DateTime(timezone=True), nullable=True)


Index("idx_job_ib", File.job_id)


class Job(db.Model):
    __tablename__ = "jobs"

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    last_state = db.Column(db.Enum(JobStatus), default=JobStatus.INITIATED)
    error = db.Column(db.Enum(JobError), default=JobError.NONE)
    info = db.Column(db.JSON)
    user = db.Column(db.String)
    source_path = db.Column(db.String)
    bucket = db.Column(db.String)
    prefix = db.Column(db.String)
    regexp = db.Column(db.String)
    creation_date = db.Column(db.DateTime(
        timezone=True), server_default=func.now())


class UserBucket(db.Model):
    __tablename__ = "user_bucket"
    __table_args__ = (
        UniqueConstraint("user", "bucket", "allowed_root_dirs"),
    )

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    user = db.Column(db.String)
    bucket = db.Column(db.String)
    allowed_root_dirs = db.Column(db.String, default="")
