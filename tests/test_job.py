import pytest
from app.enum_types import JobError, JobStatus, ItemStatus
from app.worker import _init_and_upload, _resume
from app.models import Item
from app import fs, db


@pytest.mark.parametrize("n_procs", [1, 4])
def test_ok_job(app, mock_file_tree, n_procs):

    with app.app_context():
        fs.n_procs = n_procs
        job = _init_and_upload("file:///root/path/project/",
                               "s3://bucket/project/")
        items = db.session.query(Item).where(Item.job_id == job.id)
        assert job.last_state == JobStatus.DONE
        assert job.error == JobError.NONE
        assert all(item.status == ItemStatus.TRANSFERRED for item in items)


@pytest.mark.parametrize("n_procs", [1, 4])
def test_resume_job(app, mock_file_tree, n_procs):

    with app.app_context():
        fs.n_procs = n_procs
        job = _init_and_upload("file:///root/path/project/",
                               "s3://bucket/project/")
        # simulate failed job with one item pending
        item = db.session.query(Item).where(Item.job_id == job.id).first()
        item.status = ItemStatus.PENDING
        job.error = JobError.TRANSFER_ERROR
        job.last_state = JobStatus.TRANSFERRING
        db.session.commit()

        _resume(job.id)
