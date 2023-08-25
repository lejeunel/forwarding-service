from app.enum_types import JobError
from app.base import BaseWriter
from app.exceptions import AuthenticationError


class MockBadAuthWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(message="bad auth", operation="authentication")


def test_auth_fail(agent):
    agent.uploader.writer = MockBadAuthWriter()
    job = agent.init_job("file:///root/path/project/", "s3://bucket/project/")
    items = agent.parse_and_commit_items(job.id)
    job = agent.upload(job.id)

    assert job.error == JobError.AUTH_ERROR
    assert job.info["message"] == "bad auth"
