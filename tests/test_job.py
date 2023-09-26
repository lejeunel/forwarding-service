import pytest
from src.enum_types import JobError, JobStatus, ItemStatus


@pytest.mark.parametrize(
    "n_procs,in_,out_",
    [
        (1, "file:///root/path/project/", "s3://bucket/project/"),
        (2, "file:///root/path/project/", "s3://bucket/project/"),
    ],
)
def test_multiple_files_job(job_manager, n_procs, in_, out_):

    job_manager.n_procs = n_procs
    job = job_manager.init(in_, out_)
    items = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)
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
            2,
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
def test_single_file_job(job_manager, n_procs, in_, out_):

    job_manager.n_procs = n_procs
    expected_out = "s3://bucket/project/file_1.ext"
    job = job_manager.init(in_, out_)
    items = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)
    assert len(items) == 1
    item = items[0]

    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE

    assert item.status == ItemStatus.TRANSFERRED
    assert item.out_uri == expected_out


@pytest.mark.parametrize("n_procs", [1, 4])
def test_resume_job(job_manager, n_procs):
    job_manager.n_procs = n_procs
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    items = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)

    # simulate failed job with one item pending
    item = job.items[0]

    item.status = ItemStatus.PENDING
    job.error = JobError.TRANSFER_ERROR
    job.last_state = JobStatus.PARSED
    job_manager.session.commit()

    job = job_manager.resume(job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all([i.status == ItemStatus.TRANSFERRED for i in job.items])
