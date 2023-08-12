from moto.core import set_initial_no_auth_action_count
from . import upload
from unittest.mock import patch


@set_initial_no_auth_action_count(4)
def test_refresh_credentials(app, iam_keys, bucket, mock_file_tree):
    """
    Job starts with unauthenticated s3 client.
    Check that "correct" credentials are fetched when job starts.
    """

    from fsapp.models import Job, JobError, FileStatus, File
    from fsapp import s3

    class TestAuthenticator:
        def __call__(self):
            return {
                "aws_access_key_id": iam_keys["AccessKeyId"],
                "aws_secret_access_key": iam_keys["SecretAccessKey"],
            }

    s3.client._request_signer._credentials.access_key = "invalid"
    s3.authenticator = TestAuthenticator()
    upload()
    job = Job.query.first()
    files = File.query.all()
    status = list(set([f.status for f in files]))

    assert job.error == JobError.NONE
    assert len(status) == 1
    assert status[0] == FileStatus.TRANSFERRED
