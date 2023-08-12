from datetime import datetime
import io
import logging
import mimetypes
import os
from typing import Optional

import boto3
import hvac
from .enum_types import JobError
from decouple import config

logger = logging.getLogger("S3Sender")


class BaseAuthenticator:
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def __call__(self):
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
        }


class VaultClient(BaseAuthenticator):
    """
    Retrieves AWS secrets from vault safe
    """

    def __init__(self, url=None, token_path=None, role_id=None, secret_id=None) -> None:
        """
        :param url: URL of Vault safe
        :param token_path: Path where token is stored
        :param role_id: ID of AppRole
        :param secret_id: Secret ID to read path
        """
        self.url = url
        self.token_path = token_path
        self.role_id = role_id
        self.secret_id = secret_id
        self.client = hvac.Client(url=url)

    def __call__(self):
        self.client.auth.approle.login(
            role_id=self.role_id,
            secret_id=self.secret_id,
        )

        creds = self.client.read(self.token_path)
        return {
            "aws_access_key_id": creds["data"]["aws_key"],
            "aws_secret_access_key": creds["data"]["aws_secret"],
        }


class S3Sender:

    def __init__(self, client, authenticator=BaseAuthenticator):

        self.client = client
        self.authenticator = authenticator

    def refresh_credentials(self):
        creds = self.authenticator()
        self.client = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
        )

    def put_object(
            self, body, bucket, key, contentType=None, checksumAlgorithm=None
    ):
        return self.client.put_object(
            Body=body, Bucket=bucket, Key=key, ContentType=contentType,
            ChecksumAlgorithm=checksumAlgorithm
        )

    def load_fileobj(self, path: str, extra_args={}):
        with open(path, "rb") as f:
            fileobj = io.BytesIO(f.read())

        mimetype, _ = mimetypes.guess_type(path)
        if mimetype:
            extra_args.update({"ContentType": mimetype})

        return fileobj, extra_args

    def head_object(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key)


class S3SenderExtension(S3Sender):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        AUTH_MODE = app.config.get('AUTH_MODE', None)
        if AUTH_MODE == None:
            client = boto3.client("s3")
            super().__init__(client, BaseAuthenticator())
        elif AUTH_MODE == 'AWS_CREDS':
            creds = {'aws_access_key_id': config("AWS_ACCESS_KEY_ID"),
                     'aws_secret_access_key': config("AWS_SECRET_ACCESS_KEY")}
            client = boto3.client(
                "s3",
                **creds
            )
            authenticator = BaseAuthenticator(**creds)
            super().__init__(client, authenticator)
        elif AUTH_MODE == 'VAULT':
            authenticator = VaultClient(app.config['VAULT_URL'], app.config['VAULT_TOKEN_PATH'],
                                        app.config['VAULT_ROLE_ID'], app.config['VAULT_SECRET_ID'])
            creds = authenticator()
            super().__init__(boto3.client('s3', **creds), authenticator)
