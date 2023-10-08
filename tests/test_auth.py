from forwarding_service.enum_types import JobError
from forwarding_service.base import BaseWriter
from forwarding_service.exceptions import AuthenticationError, TransferError


class MockBadAuthWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(message="bad auth", operation="authentication")

class MockTransferErrorWriter(BaseWriter):

    def __call__(self, *args, **kwargs):
        raise TransferError(message="bad auth", operation="transfer")

def test_auth_fail(job_manager):
    job_manager.reader_writer.writer = MockBadAuthWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job = job_manager.run(job.id)

    assert job.error == JobError.AUTH_ERROR
    assert job.info["message"] == "bad auth"

def test_transfer_error(job_manager):
    job_manager.reader_writer.writer = MockTransferErrorWriter()
    job_manager.n_procs = 1
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job.id)
    job = job_manager.run(job.id)

    assert job.error == JobError.TRANSFER_ERROR
    assert job.info["message"] == "bad auth"
