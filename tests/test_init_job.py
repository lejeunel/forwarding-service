from src.enum_types import JobError

def test_existing_source(job_manager):
    job = job_manager.init(
        "file:///root/path/project/",
        "s3://bucket/path/project/",
    )
    assert job.error == JobError.NONE

def test_non_existing_source_must_fail(job_manager):
    job = job_manager.init(
        "file:///root/path/non-existing-project/",
        "s3://bucket/non-existing-project/",
    )
    assert job.error == JobError.INIT_ERROR
