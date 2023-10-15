import pytest
from forwarding_service.enum_types import JobError
from forwarding_service.exceptions import InitSrcException, InitException


def test_existing_source(job_manager):
    job = job_manager.init(
        "file:///root/path/project/",
        "s3://bucket/path/project/",
    )
    assert job.error == JobError.NONE

def test_non_existing_source_must_fail(job_manager):
    with pytest.raises(InitSrcException):
        job_manager.init(
            "file:///root/path/non-existing-project/",
            "s3://bucket/non-existing-project/",
        )

def test_empty_source(job_manager):
    with pytest.raises(InitSrcException):
        job_manager.init(
            "file:///root/path/emptydir/",
            "s3://bucket/project/",
        )


@pytest.mark.parametrize(
    "in_,out_",
    [
        ("http://root/path/project/", "s3://bucket/project/"),
        ("/root/path/project/", "s3://bucket/project/"),
        ("file:///root/path/project/", "bucket/project/"),
    ],
)
def test_invalid_specs_must_fail(job_manager, in_, out_):
    with pytest.raises(InitException):
        job_manager.init(
            in_,
            out_,
        )
