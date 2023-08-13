from urllib.parse import urlparse
from decouple import config
from .auth import S3StaticCredentials, VaultCredentials
from .file import FileSystemReader
from .s3 import S3Writer
from .uploader import Uploader


class UploaderExtension(Uploader):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.auth_mode = app.config.get("UPLOADER_AUTH_MODE", None)
        self.do_checksum = app.config.get("UPLOADER_CHECKSUM", False)
        self.n_procs = app.config.get("UPLOADER_N_PROCS", 1)

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
            if self.auth_mode == "env":
                creds = {
                    "aws_access_key_id": config("AWS_ACCESS_KEY_ID"),
                    "aws_secret_access_key": config("AWS_SECRET_ACCESS_KEY"),
                }
                self.authenticator = S3StaticCredentials(**creds)
            elif self.auth_mode == "profile":
                self.authenticator = S3StaticCredentials()
            elif self.auth_mode == "vault":
                self.authenticator = VaultCredentials(
                    config("VAULT_URL"),
                    config("VAULT_TOKEN_PATH"),
                    config("VAULT_ROLE_ID"),
                    config("VAULT_SECRET_ID"),
                )

            self.writer = S3Writer(self.authenticator)
        else:
            raise NotImplementedError
