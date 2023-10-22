#!/usr/bin/env python3
import io
from urllib.parse import urlparse

import pytest
from forwarding_service.base import BaseReader, BaseWriter
from forwarding_service.transfer_agent import TransferAgent
from forwarding_service.enum_types import ItemStatus, JobError, JobStatus
from forwarding_service.job_manager import JobManager
from sqlmodel import Session, SQLModel, create_engine


class MockReader(BaseReader):
    files = [f for f in ["file_{}.ext".format(i) for i in range(10)]]
    tree = {
        "": {
            "root": {
                "path": {
                    "project": {"": files},
                    "emptydir": {"": []},
                    "otherproject": {"": files},
                }
            }
        }
    }

    def read(self, *args, **kwargs):
        return io.BytesIO(bytes("test", "ascii"))

    def exists(self, uri):
        path = urlparse(uri).path
        curr = self.tree
        for node in path.split("/"):
            if node in curr:
                curr = curr[node]
            elif "" in curr and isinstance(curr[""], list):
                if node in curr[""]:
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
    def __init__(self):
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1
        pass

    def refresh_credentials(self):
        pass


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    yield engine

    SQLModel.metadata.drop_all(engine)


@pytest.fixture
def session(engine):
    session = Session(engine)
    yield session


@pytest.fixture
def job_manager(session):
    tfa = TransferAgent(reader=MockReader(), writer=MockWriter())
    job_manager = JobManager(session=session, transfer_agent=tfa)

    yield job_manager


@pytest.fixture
def completed_job(job_manager):
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job)
    job_manager.run(job)

    yield job


@pytest.fixture
def failed_job(session, completed_job):
    """simulate failed job with one item pending"""

    item = completed_job.items[0]

    job = completed_job
    item.status = ItemStatus.PENDING
    job.error = JobError.TRANSFER_ERROR
    job.status = JobStatus.PARSED
    session.commit()

    yield job
