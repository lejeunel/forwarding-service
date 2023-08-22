import pytest
from app.enum_types import JobError, JobStatus, ItemStatus


@pytest.mark.parametrize(
    "n_procs,in_,out_",
    [
        (1, "file:///root/path/project/", "s3://bucket/project/"),
        # (2, "file:///root/path/project/", "s3://bucket/project/"),
    ],
)
def test_multiple_files_job(agent, n_procs, in_, out_):

    agent.n_procs = n_procs
    job = agent.init_job(in_, out_)
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)
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
        # (
        #     2,
        #     "file:///root/path/project/file_1.ext",
        #     "s3://bucket/project/file_1.ext"
        # ),
        (
            1,
            "file:///root/path/project/file_1.ext",
            "s3://bucket/project/"
        ),
    ],
)
def test_single_file_job(agent, n_procs, in_, out_):

    agent.n_procs = n_procs
    expected_out = "s3://bucket/project/file_1.ext"
    job = agent.init_job(in_, out_)
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)
    assert len(items) == 1
    item = items[0]

    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE

    assert item.status == ItemStatus.TRANSFERRED
    assert item.out_uri == expected_out


# @pytest.mark.parametrize("n_procs", [1, 4])
@pytest.mark.parametrize("n_procs", [1])
def test_resume_job(agent, session, n_procs):
    agent.n_procs = n_procs
    job = agent.init_job("file:///root/path/project/", "s3://bucket/project/")
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)

    # simulate failed job with one item pending
    from app.models import Item, Job
    item = job.items[0]

    item.status = ItemStatus.PENDING
    job.error = JobError.TRANSFER_ERROR
    job.last_state = JobStatus.TRANSFERRING
    session.commit()

    job = agent.resume(job.id)
    assert job.last_state == JobStatus.DONE
    assert job.error == JobError.NONE
    assert all([i.status == ItemStatus.TRANSFERRED for i in job.items])
