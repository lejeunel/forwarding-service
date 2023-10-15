from forwarding_service.enum_types import JobError, ItemStatus
from forwarding_service.base import BaseWriter
from forwarding_service.exceptions import (
    AuthenticationError,
    RemoteException,
    CheckSumException,
    TransferException,
)
import pytest


class MockBadAuthWriter(BaseWriter):
    def __call__(self):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(error="bad auth", operation="authentication")


class MockTransferErrorWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        raise TransferException(error="bad auth", operation="transfer")


class MockChecksumErrorWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        raise CheckSumException(error="bad auth", operation="checksum")


def test_auth_fail(job_manager):
    job_manager.batch_rw.writer = MockBadAuthWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")

    with pytest.raises(AuthenticationError):
        job_manager.run(job)

        assert job.error == JobError.AUTH_ERROR


@pytest.mark.parametrize(
    "n_threads",
    [1, 2],
)
def test_transfer_error(job_manager, n_threads):
    job_manager.batch_rw.writer = MockTransferErrorWriter()
    job_manager.batch_rw.n_threads = n_threads
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job)
    with pytest.raises(RemoteException):
        job = job_manager.run(job)

    assert job.error == JobError.TRANSFER_ERROR
    assert all([item.status == ItemStatus.PENDING for item in job.items])


@pytest.mark.parametrize(
    "n_threads",
    [1, 2],
)
def test_checksum_error(job_manager, n_threads):
    job_manager.batch_rw.writer = MockChecksumErrorWriter()
    job_manager.batch_rw.n_threads = n_threads
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job)
    with pytest.raises(RemoteException):
        job = job_manager.run(job)
    assert job.error == JobError.CHECKSUM_ERROR
    assert all([item.status == ItemStatus.PENDING for item in job.items])
