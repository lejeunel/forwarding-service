from forwarding_service.enum_types import JobError
from forwarding_service.base import BaseWriter
from forwarding_service.exceptions import AuthenticationError, TransferError, CheckSumError
import pytest


class MockBadAuthWriter(BaseWriter):
    def __call__(self):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(message="bad auth", operation="authentication")

class MockTransferErrorWriter(BaseWriter):

    def __call__(self, *args, **kwargs):
        raise TransferError(message="bad auth", operation="transfer")

class MockChecksumErrorWriter(BaseWriter):

    def __call__(self, *args, **kwargs):
        raise CheckSumError(message="bad auth", operation="checksum")

def test_auth_fail(job_manager):
    job_manager.reader_writer.writer = MockBadAuthWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")

    with pytest.raises(AuthenticationError):
        job_manager.run(job)

        assert job.error == JobError.AUTH_ERROR

def test_transfer_error(job_manager):
    job_manager.reader_writer.writer = MockTransferErrorWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job)
    with pytest.raises(TransferError):
        job = job_manager.run(job)
        assert job.error == JobError.TRANSFER_ERROR

def test_checksum_error(job_manager):
    job_manager.reader_writer.writer = MockChecksumErrorWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job)
    with pytest.raises(CheckSumError):
        job = job_manager.run(job)
        assert job.error == JobError.CHECKSUM_ERROR
