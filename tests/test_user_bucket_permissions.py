from fsapp.command import add_user
from moto.core import set_initial_no_auth_action_count
from fsapp.models import Job, JobError, FileStatus, File

from . import upload


@set_initial_no_auth_action_count(4)
def test_not_allowed_bucket(app, bucket, mock_file_tree, mock_refresh_credentials):

    upload(bucket="notmybucket")
    job = Job.query.first()
    assert job.error == JobError.INIT_ERROR
    assert "not allowed to bucket" in job.info["message"][0]


@set_initial_no_auth_action_count(4)
def test_user_not_allowed_from_dir(
    app, bucket, mock_file_tree, mock_refresh_credentials
):

    user = "new-test-user"
    dir_ok = "/fileshares/hcs-research11"
    add_user(user, "mybucket", dir_ok)
    dir_not_ok = "/fileshares/hcs-research10"
    upload(user=user, src=dir_not_ok)
    job = Job.query.first()
    assert job.error == JobError.INIT_ERROR
    assert "not allowed from" in job.info["message"][0]
