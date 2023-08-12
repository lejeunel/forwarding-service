from moto.core import set_initial_no_auth_action_count
from . import upload
from unittest.mock import patch
from hvac.exceptions import VaultError


@set_initial_no_auth_action_count(4)
def test_vault(app, iam_keys, bucket, mock_file_tree):
    """
    Job starts with unauthenticated s3 client.
    Checks that vault exception is raised and appears in job info
    """

    from fsapp.models import Job, JobError, FileStatus, File

    def raise_vault_error(*args, **kwargs):
        raise VaultError(message="some vault error",
                         errors="vault error message")

    with patch(
        "fsapp.sender.BaseAuthenticator.__call__",
        wraps=raise_vault_error,
    ) as m:
        upload()
        job = Job.query.first()

        assert job.error == JobError.VAULT_ERROR
