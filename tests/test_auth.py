from forwarding_service.enum_types import JobError
from forwarding_service.base import BaseWriter
from forwarding_service.exceptions import AuthenticationError


class MockBadAuthWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(message="bad auth", operation="authentication")


def test_auth_fail(job_manager):
    job_manager.reader_writer.writer = MockBadAuthWriter()
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job = job_manager.run(job.id)

    assert job.error == JobError.AUTH_ERROR
    assert job.info["message"] == "bad auth"
