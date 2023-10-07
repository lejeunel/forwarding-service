import pytest
from forwarding_service.enum_types import ItemStatus, JobError, JobStatus
from forwarding_service.query import ItemQueryArgs, JobQueryArgs
import uuid


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
        (1, "file:///root/path/project/file_1.ext", "s3://bucket/project/file_1.ext"),
        (2, "file:///root/path/project/file_1.ext", "s3://bucket/project/file_1.ext"),
        (1, "file:///root/path/project/file_1.ext", "s3://bucket/project/"),
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


def test_resume_job(job_manager, partial_job):
    job = job_manager.resume(partial_job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all([i.status == ItemStatus.TRANSFERRED for i in job.items])


def test_job_exists(job_manager, completed_job):
    assert job_manager.query.job_exists(completed_job.id)

    non_existing_id = uuid.uuid4()
    assert job_manager.query.job_exists(non_existing_id) == False


def test_invalid_id_raises_exception(job_manager):
    with pytest.raises(Exception):
        job_manager.query.job_exists("-")

    with pytest.raises(Exception):
        job_manager.query.items(job_id="-")


def test_get_items(job_manager, completed_job):
    empty_job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    assert len(job_manager.query.items(ItemQueryArgs(job_id=completed_job.id))) > 0
    assert len(job_manager.query.items(ItemQueryArgs(job_id=empty_job.id))) == 0


def test_get_jobs(job_manager, completed_job):
    initiated_job = job_manager.init(
        "file:///root/path/project/", "s3://bucket/project/"
    )
    assert (
        job_manager.query.jobs(JobQueryArgs(status=JobStatus.INITIATED))[0].id
        == initiated_job.id
    )
    assert (
        job_manager.query.jobs(JobQueryArgs(status=JobStatus.DONE))[0].id
        == completed_job.id
    )


def test_delete_job(job_manager, completed_job):
    job_manager.delete_job(completed_job.id)
    assert job_manager.query.job_exists(completed_job.id) == False
    assert len(job_manager.query.items(ItemQueryArgs(job_id=completed_job.id))) == 0
