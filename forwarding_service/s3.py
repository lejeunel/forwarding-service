from urllib.parse import urlparse

import boto3
from aws_error_utils import get_aws_error_info
from botocore.client import ClientError as BotoClientError

from .base import BaseWriter
from .exceptions import TransferException


class S3Writer(BaseWriter):
    def __init__(self, session, *args, **kwargs):
        self.session = session
        self.client = self.session.client('s3')

    @classmethod
    def from_profile_name(cls, profile_name):
        session = boto3.Session(profile_name=profile_name)
        self.client = session.client('s3')

    @classmethod
    def from_auth_client(cls, auth_client):
        creds = auth_client.get_credentials()
        session = boto3.Session(aws_access_key_id=creds['aws_access_key_id'],
                                   aws_secret_access_key=creds['aws_secret_access_key'])
        writer = cls(session)
        return writer

    def __call__(
        self,
        bytes_,
        uri,
        mime_type=None,
        checksum=None,
    ):
        uri = urlparse(uri)

        try:
            return self.client.put_object(
                Body=bytes_,
                Bucket=uri.netloc,
                Key=uri.path,
                ContentType=mime_type if mime_type else '',
                ChecksumAlgorithm="SHA256",
                ChecksumSHA256=checksum,
            )
        except BotoClientError as e:
            e = get_aws_error_info(e)
            raise TransferException(error=e.message, operation=e.operation_name)

    def refresh_credentials(self):
        pass
