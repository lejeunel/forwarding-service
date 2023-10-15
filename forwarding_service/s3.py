from botocore.client import ClientError as BotoClientError
import boto3
from .base import BaseWriter
import copy
from urllib.parse import urlparse
from .auth import BaseAuthenticator
from .exceptions import TransferException
from aws_error_utils import get_aws_error_info


class S3Writer(BaseWriter):
    def __init__(self, profile_name='default', *args, **kwargs):
        self.session = boto3.Session(profile_name=profile_name)
        self.client = self.session.client('s3')

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
