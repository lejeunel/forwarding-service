import boto3
from .base import BaseWriter
import copy
from urllib.parse import urlparse
from .auth import BaseAuthenticator


class S3Writer(BaseWriter):
    def __init__(self, authenticator=BaseAuthenticator):
        self.client = boto3.client('s3')
        self.authenticator = authenticator

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k != 'client':
                setattr(result, k, copy.deepcopy(v, memo))
        setattr(result, 'client', boto3.client('s3'))

    def __reduce__(self):
        return (self.__class__, (self.authenticator,))

    def __call__(
            self, bytes_, uri, mime_type=None, checksum=None,
    ):
        uri = urlparse(uri)

        return self.client.put_object(
            Body=bytes_, Bucket=uri.netloc, Key=uri.path, ContentType=mime_type,
            ChecksumAlgorithm='SHA256',
            ChecksumSHA256=checksum
        )

    def refresh_credentials(self):
        creds = self.authenticator()
        self.client = boto3.client("s3", **creds)
