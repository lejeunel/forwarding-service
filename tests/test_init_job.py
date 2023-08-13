import pytest
from app.enum_types import JobError
from app.worker import _init_and_upload



def test_non_existing_source_must_fail(app, mock_file_tree):

    job = _init_and_upload(
        "file:///root/path/non-existing-project/", "s3://bucket/non-existing-project/"
    )
    assert job.error == JobError.INIT_ERROR
