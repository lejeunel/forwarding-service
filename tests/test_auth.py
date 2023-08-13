from app.worker import _init_and_upload
from app.enum_types import JobError
from app.uploader import BaseWriter
from app.exceptions import AuthenticationError


class MockBadAuthWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        raise AuthenticationError(
            message='bad auth', operation='authentication')


def test_auth_fail(app, mock_file_tree):

    from app import fs
    with app.app_context():
        fs.writer = MockBadAuthWriter()
        job = _init_and_upload("file:///root/path/project/",
                               "s3://bucket/project/")
        assert job.error == JobError.AUTH_ERROR
