from app.worker import _upload
from app.enum_types import JobError


def test_trailing_slashes_must_match(app, mock_file_tree):

    job = _upload("/root/path/project/", "s3://bucket/file.ext")
    assert job.error == JobError.INIT_ERROR


def test_single_file_must_fail(app, mock_file_tree):

    job = _upload("/root/path/project/myfile.ext", "s3://bucket/file.ext")
    assert job.error == JobError.INIT_ERROR


def test_non_existing_source_must_fail(app, mock_file_tree):

    job = _upload("/root/path/non-existing-project/",
                  "s3://bucket/non-existing-project/")
    assert job.error == JobError.INIT_ERROR
