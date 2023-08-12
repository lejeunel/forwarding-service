from moto.core import set_initial_no_auth_action_count
from . import upload


@set_initial_no_auth_action_count(4)
def test_integrity(
    app, bucket, mock_file_tree, mock_wrong_checksum, mock_refresh_credentials
):

    from fsapp.models import Job, JobError, FileStatus, File

    upload()
    job = Job.query.first()
    files = File.query.all()
    status = set([f.status for f in files])

    assert len(status) == 2
    assert FileStatus.PENDING in status
    assert FileStatus.ERROR in status
    assert job.error == JobError.CHECKSUM_ERROR
