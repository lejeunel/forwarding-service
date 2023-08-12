#!/usr/bin/env python3
import hashlib
import io
import json
import os
import uuid
from pathlib import Path
from unittest.mock import patch

import boto3
import moto
import pytest

from . import mock_tree
from app.uploader import BaseWriter


class MockWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass


@pytest.fixture()
def mock_file_tree():

    with mock_tree as m:
        yield m


def mock_bytes(*args, **kwargs):

    return io.BytesIO(bytes("test", "ascii"))


@pytest.fixture(autouse=True)
def mock_read_file():

    with patch(
        "app.uploader.FileSystemReader.read",
        wraps=mock_bytes,
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_uploader_setup():

    def void(*args, **kwargs):
        pass

    with patch(
        "app.uploader.UploaderExtension.setup",
        wraps=void,
    ) as m:
        yield m


@pytest.fixture
def app():
    from app import create_app, fs, db
    from app.uploader import FileSystemReader

    app = create_app(mode='test')
    with app.app_context():
        fs.reader = FileSystemReader()
        fs.writer = MockWriter()

        db.create_all()

        yield app

        db.drop_all()
