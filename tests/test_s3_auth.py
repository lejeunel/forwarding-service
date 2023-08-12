from moto.core import set_initial_no_auth_action_count
from . import upload, mock_tree
import boto3
import moto
import pytest


@set_initial_no_auth_action_count(4)
def test_correct_credentials(app, bucket, mock_file_tree, mock_refresh_credentials):

    from fsapp.models import Job, JobError, FileStatus, File
    from fsapp.enum_types import JobStatus
    from fsapp import s3

    upload()
    job = Job.query.first()

    assert job.error == JobError.NONE
    assert job.last_state == JobStatus.DONE


@set_initial_no_auth_action_count(4)
def test_incorrect_credentials(app, bucket, mock_file_tree, mock_refresh_credentials):

    from fsapp.models import Job, JobError, FileStatus, File
    from fsapp.enum_types import JobStatus
    from fsapp import s3

    # change aws_access_key
    s3.client._request_signer._credentials.access_key = "invalid"
    upload()
    job = Job.query.first()

    assert job.error == JobError.S3_ERROR
    assert job.last_state == JobStatus.PARSED
    assert "HeadObject" in job.info["operation"]
