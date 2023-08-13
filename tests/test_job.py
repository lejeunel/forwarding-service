import pytest
from app.enum_types import JobError, JobStatus, ItemStatus
from app.worker import _init_and_upload, _resume
from app.models import Item
from app import fs, db


@pytest.mark.parametrize(
    "n_procs,in_,out_",
    [
        (1, "file:///root/path/project/", "s3://bucket/project/"),
        (4, "file:///root/path/project/", "s3://bucket/project/"),
    ],
)
def test_multiple_files_job(app, mock_file_tree, n_procs, in_, out_):
    with app.app_context():
        fs.n_procs = n_procs
        job = _init_and_upload(in_, out_)
        items = db.session.query(Item).where(Item.job_id == job.id)
        assert job.last_state == JobStatus.DONE
        assert job.error == JobError.NONE
        assert all(item.status == ItemStatus.TRANSFERRED for item in items)
        assert all(item.transferred > item.created for item in items)


@pytest.mark.parametrize(
    "n_procs,in_,out_",
    [
        (
            1,
            "file:///root/path/project/file_1.ext",
            "s3://bucket/project/file_1.ext"
        ),
        (
            4,
            "file:///root/path/project/file_1.ext",
            "s3://bucket/project/file_1.ext"
        ),
        (
            1,
            "file:///root/path/project/file_1.ext",
            "s3://bucket/project/"
        ),
    ],
)
def test_single_file_job(app, mock_file_tree, n_procs, in_, out_):

    expected_out = "s3://bucket/project/file_1.ext"
    with app.app_context():
        fs.n_procs = n_procs
        job = _init_and_upload(in_, out_)
        items = db.session.query(Item).where(Item.job_id == job.id)
        assert items.count() == 1
        item = items.first()

        assert job.last_state == JobStatus.DONE
        assert job.error == JobError.NONE

        assert item.status == ItemStatus.TRANSFERRED
        assert item.out_uri == expected_out


@pytest.mark.parametrize("n_procs", [1, 4])
def test_resume_job(app, mock_file_tree, n_procs):
    with app.app_context():
        fs.n_procs = n_procs
        job = _init_and_upload("file:///root/path/project/", "s3://bucket/project/")

        # simulate failed job with one item pending
        item = db.session.query(Item).where(Item.job_id == job.id).first()
        item.status = ItemStatus.PENDING
        job.error = JobError.TRANSFER_ERROR
        job.last_state = JobStatus.TRANSFERRING
        db.session.commit()

        _resume(job.id)
