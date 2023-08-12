from moto.core import set_initial_no_auth_action_count
from . import upload
from . import TEST_FILES
from fsapp.models import Job, JobError, FileStatus, File
from fsapp.enum_types import JobStatus
import os


@set_initial_no_auth_action_count(4)
def test_single_file(app, bucket, s3_client, mock_file_tree, mock_refresh_credentials):

    file_ = TEST_FILES[0]
    prefix = "test-prefix"
    upload(src=file_, prefix=prefix)
    job = Job.query.first()

    assert job.error == JobError.NONE
    assert File.query.count() == 1
    assert job.description["src"]

    file = File.query.first()
    s3_client.head_object(
        Bucket="mybucket",
        Key=os.path.join(file.prefix, file.filename),
    )


@set_initial_no_auth_action_count(4)
def test_src_dir(app, bucket, s3_client, mock_file_tree, mock_refresh_credentials):

    upload()
    job = Job.query.first()

    assert job.error == JobError.NONE
    for f in File.query.all():
        s3_client.head_object(
            Bucket="mybucket",
            Key=os.path.join(f.prefix, f.filename),
        )


@set_initial_no_auth_action_count(4)
def test_src_not_found(app, bucket, mock_file_tree, mock_refresh_credentials):

    upload(src="/fileshares/bla")
    job = Job.query.first()

    assert job.error == JobError.INIT_ERROR
    assert "not found" in job.info["message"][0]


@set_initial_no_auth_action_count(4)
def test_src_not_allowed_and_not_found(
    app, bucket, mock_file_tree, mock_refresh_credentials
):

    upload(src="/nonexistingdir")
    job = Job.query.first()
    assert Job.query.first().error == JobError.INIT_ERROR
    assert any("not allowed" in m for m in job.info["message"])
    assert any("not found" in m for m in job.info["message"])
