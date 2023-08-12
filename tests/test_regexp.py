from moto.core import set_initial_no_auth_action_count
from . import upload


@set_initial_no_auth_action_count(4)
def test_regexp(app, bucket, mock_file_tree, mock_refresh_credentials):

    from fsapp.models import Job, JobError, FileStatus, File
    from fsapp.enum_types import JobStatus

    upload(regexp=r".*\.jpg")
    job = Job.query.first()
    files = File.query.all()
    status = set([f.status for f in files])

    assert job.error == JobError.NONE
    assert job.last_state == JobStatus.DONE
    assert len(files) == 0
