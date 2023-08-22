from urllib.parse import urlparse
from decouple import config
from .auth import S3StaticCredentials, VaultCredentials
from .file import FileSystemReader
from .s3 import S3Writer
from .uploader import Uploader


def make_auth_uploader(
    reader: BaseReader,
    writer,
    authenticator,
    upload_auth_mode: str = "env",
    upload_checksum: bool = False,
    upload_n_procs: int = 1,
):
    reader = FileSystemReader()


class AuthUploader(Uploader):
    def __init__(
        self,
    ):
        self.upload_auth_mode = upload_auth_mode
        self.upload_checksum = upload_checksum
        self.upload_n_procs = upload_n_procs

    def setup(self, source, destination):
        """Setup reader and writer given URIs. This should be called before run."""
        in_scheme = urlparse(source).scheme
        out_scheme = urlparse(destination).scheme

        if in_scheme == "file":
            self.reader = FileSystemReader()
        else:
            raise NotImplementedError

        if out_scheme == "s3":
            self.authenticator = S3StaticCredentials()
            if self.upload_auth_mode == "env":
                creds = {
                    "aws_access_key_id": config("AWS_ACCESS_KEY_ID"),
                    "aws_secret_access_key": config("AWS_SECRET_ACCESS_KEY"),
                }
                self.authenticator = S3StaticCredentials(**creds)
            elif self.upload_auth_mode == "profile":
                self.authenticator = S3StaticCredentials()
            elif self.upload_auth_mode == "vault":
                self.authenticator = VaultCredentials(
                    config("VAULT_URL"),
                    config("VAULT_TOKEN_PATH"),
                    config("VAULT_ROLE_ID"),
                    config("VAULT_SECRET_ID"),
                )

            self.writer = S3Writer(self.authenticator)
        else:
            raise NotImplementedError
