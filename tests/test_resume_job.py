#!/usr/bin/env python3
from fsapp import db
from fsapp.enum_types import JobStatus
from fsapp.models import File, FileStatus, Job, JobError
from fsapp.schemas import JobSchema
from fsapp.worker import _resume_job, _parse_files
from moto.core import set_initial_no_auth_action_count

from . import upload


def do_failed_job():
    upload()
    job = Job.query.first()
    files = File.query.all()

    # set a "failed" state
    job.error = JobError.S3_ERROR
    job.last_state = JobStatus.PARSED
    files = files[len(files) // 2 :]
    for f in files:
        f.status = FileStatus.PENDING

    db.session.add(job)
    db.session.add_all(files)
    db.session.commit()


@set_initial_no_auth_action_count(4)
def test_resume_failed_job(app, bucket, mock_file_tree, mock_refresh_credentials):

    do_failed_job()
    job = Job.query.first()

    _resume_job("", job.id, testing=True)
    job = Job.query.first()
    status = [f.status for f in File.query.all()]
    assert job.last_state == JobStatus.DONE


@set_initial_no_auth_action_count(4)
def test_resume_ignores_transferred_files(
    app, bucket, mock_file_tree, mock_refresh_credentials
):
    """
    File parsing should ignore files based on unique constraint,
    i.e. keep existing records
    """
    do_failed_job()
    job = Job.query.first()
    files = _parse_files(job.id)
    status = list(set([f.status for f in files]))
    assert FileStatus.TRANSFERRED in status
    assert FileStatus.PENDING in status
    assert len(status) == 2
