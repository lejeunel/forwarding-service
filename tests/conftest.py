#!/usr/bin/env python3
import io
from urllib.parse import urlparse

import pytest
from app.base import BaseReader, BaseWriter
from app.item_uploader import ItemUploader
from app.models import BaseModel
from app.transfer_agent import TransferAgent
from app import make_session


class MockReader(BaseReader):
    files = [f for f in ["file_{}.ext".format(i) for i in range(10)]]
    tree = {"": {"root": {"path": {"project": {"": files}, "otherproject": {"": files}}}}}

    def read(self, *args, **kwargs):
        return io.BytesIO(bytes("test", "ascii"))

    def exists(self, uri):
        path = urlparse(uri).path
        curr = self.tree
        for node in path.split('/'):
            if node in curr:
                curr = curr[node]
            elif '' in curr and isinstance(curr[''], list):
                if node in curr['']:
                    return [node]

            else:
                return []
        return curr


    def list(self, uri, *args, **kwargs):
        return self.exists(uri)

    def __call__(self, *args, **kwargs):
        return self.read(), "image/tiff"

    def refresh_credentials(self):
        pass


class MockWriter(BaseWriter):
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        pass


@pytest.fixture
def agent():
    uploader = ItemUploader(reader=MockReader(), writer=MockWriter())
    session = make_session('sqlite://')
    agent = TransferAgent(session=session, uploader=uploader)

    BaseModel.metadata.create_all(agent.session.bind.engine)

    yield agent

    BaseModel.metadata.drop_all(agent.session.bind.engine)
