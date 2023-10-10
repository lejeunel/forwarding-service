import pytest
from forwarding_service.enum_types import ItemStatus, JobError, JobStatus
from forwarding_service.exceptions import InitError
from forwarding_service.models import Item, Job
from forwarding_service.query import JobQueryArgs, Query


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
    job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all(item.status == ItemStatus.TRANSFERRED for item in job.items)
    assert all(item.transferred_at > item.created_at for item in job.items)


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
    job = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)
    assert len(job.items) == 1
    item = job.items[0]

    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE

    assert item.status == ItemStatus.TRANSFERRED
    assert item.out_uri == expected_out


def test_duplicate_job(job_manager, completed_job):
    with pytest.raises(InitError):
        job_manager.init(completed_job.source, completed_job.destination)


def test_resume_failed_job(job_manager, partial_job):
    job = job_manager.resume(partial_job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all([i.status == ItemStatus.TRANSFERRED for i in job.items])


def test_resume_completed_job(job_manager, completed_job):
    job = job_manager.resume(completed_job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all([i.status == ItemStatus.TRANSFERRED for i in job.items])


def test_job_exists(session, completed_job):

    query = Query(session, Job)
    assert query.exists(completed_job.id)

def test_invalid_id_raises_exception(job_manager):
    with pytest.raises(Exception):
        job_manager.query.items(job_id="-")


def test_get_items(session, completed_job):

    query = Query(session, Job)

    assert len(query.get(JobQueryArgs(id=completed_job.id))) > 0


def test_get_jobs(session, completed_job):
    query = Query(session, Job)
    result = query.get(JobQueryArgs(status=JobStatus.DONE))
    assert result[0].id == completed_job.id


def test_delete_job(session, completed_job):
    query = Query(session, Job)
    id = completed_job.id
    query.delete(JobQueryArgs(id=id))

    jobs = Query(session, Job).get(JobQueryArgs(id=id))
    items = Query(session, Item).get()
    assert len(jobs) == 0
    assert len(items) == 0
