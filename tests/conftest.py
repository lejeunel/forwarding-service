#!/usr/bin/env python3
import io
from urllib.parse import urlparse

import pytest
from app.base import BaseReader, BaseWriter
from app.models import Base
from app.transfer_agent import TransferAgent
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


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


@pytest.fixture(scope="session")
def engine():
    return create_engine("sqlite:///")


@pytest.fixture(scope="session")
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def session(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()


@pytest.fixture
def agent(engine, tables, session):
    agent = TransferAgent(session, reader=MockReader(), writer=MockWriter())

    yield agent
