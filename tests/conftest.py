#!/usr/bin/env python3
import hashlib
import io
import os
import uuid
from unittest.mock import patch
import boto3
import json
from pathlib import Path

import pytest

import moto
from . import user, bucket, mock_tree


@pytest.fixture()
def mock_file_tree():

    with mock_tree as m:
        yield m


def mock_bytes(*args, **kwargs):

    return (io.BytesIO(bytes("test", "ascii")), None)


@pytest.fixture
def mock_refresh_credentials():
    with patch("fsapp.sender.S3SenderExtension.refresh_credentials") as m:
        yield m


@pytest.fixture(autouse=True)
def mock_load_fileobj():

    fileobj = mock_bytes()
    with patch(
        "fsapp.sender.S3SenderExtension.load_fileobj",
        wraps=mock_bytes,
    ) as m:
        yield m


@pytest.fixture
def mock_wrong_checksum(*args, **kwargs):
    fileobj = mock_bytes()[0]
    # compute md5 checksum
    md5_checksum = hashlib.md5(fileobj.getbuffer()).hexdigest()

    # corrupt checksum
    last = md5_checksum[-1]
    # converting char into int and add 1
    changed_char = chr(ord(last) + 1)
    md5_wrong_checksum = md5_checksum[:-1] + changed_char

    with patch(
        "fsapp.sender.S3SenderExtension.head_object",
        return_value={"ETag": md5_wrong_checksum},
    ) as m:
        yield m


@pytest.fixture
def iam_keys():
    """
    Should create a user with attached policy allowing read/write operations on S3.
    """
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
    }

    with moto.mock_iam():
        # Create client and user
        client = boto3.client("iam", region_name="us-east-1")
        client.create_user(UserName=user)

        # Create and attach the policy
        policy_arn = client.create_policy(
            PolicyName="policy1", PolicyDocument=json.dumps(policy_document)
        )["Policy"]["Arn"]
        client.attach_user_policy(UserName=user, PolicyArn=policy_arn)

        # Return the access keys
        yield client.create_access_key(UserName=user)["AccessKey"]


@pytest.fixture
def bucket(s3_client):
    s3_client.create_bucket(Bucket=bucket)
    yield


@pytest.fixture
def s3_client(iam_keys):
    with moto.mock_s3():
        conn = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=iam_keys["AccessKeyId"],
            aws_secret_access_key=iam_keys["SecretAccessKey"],
        )
        yield conn


@pytest.fixture
def app(s3_client):
    import flask
    import fsapp
    from fsapp import db, s3, ma, fs
    from fsapp.models import UserBucket

    app = flask.Flask(__name__)
    app.config.from_object("fsapp.config.test")
    db.init_app(app)
    s3.init_app(
        app,
        s3_client,
    )
    ma.init_app(app)
    fs.init_app(app)

    with app.app_context():
        fsapp.db.drop_all()
        fsapp.db.create_all()
        user = UserBucket(
            user=user,
            bucket=bucket,
            allowed_root_dirs="/fileshares",
        )
        fsapp.db.session.add(user)
        fsapp.db.session.commit()

        yield app
