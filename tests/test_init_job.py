from forwarding_service.enum_types import JobError
from forwarding_service.exceptions import InitError
from pydantic_core import ValidationError
import pytest

def test_existing_source(job_manager):
    job = job_manager.init(
        "file:///root/path/project/",
        "s3://bucket/path/project/",
    )
    assert job.error == JobError.NONE

def test_non_existing_source_must_fail(job_manager):
    with pytest.raises(InitError):
        job = job_manager.init(
            "file:///root/path/non-existing-project/",
            "s3://bucket/non-existing-project/",
        )

def test_wrong_format_must_fail(job_manager):
    with pytest.raises(ValidationError):
        job_manager.init(
            "/root/path/non-existing-project/",
            "s3://bucket/non-existing-project/",
        )
