from app.worker import _upload
from app.enum_types import JobError
import pytest


@pytest.mark.parametrize("in_uri,out_uri", [("file:///root/path/project/", "s3://bucket/file.ext"),
                                            ("/root/path/project/myfile.ext", "s3://bucket/file.ext")])
def test_mismatch_slash_and_single_file(app, mock_file_tree, in_uri, out_uri):

    job = _upload(in_uri, out_uri)
    assert job.error == JobError.INIT_ERROR


def test_non_existing_source_must_fail(app, mock_file_tree):

    job = _upload("/root/path/non-existing-project/",
                  "s3://bucket/non-existing-project/")
    assert job.error == JobError.INIT_ERROR
