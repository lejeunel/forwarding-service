#!/usr/bin/env python3
import io
from urllib.parse import urlparse

import pytest
from forwarding_service.base import BaseReader, BaseWriter
from forwarding_service.reader_writer import ReaderWriter
from forwarding_service.models import BaseModel
from forwarding_service.job_manager import JobManager
from forwarding_service import make_session
from forwarding_service.enum_types import JobError, JobStatus, ItemStatus


class MockReader(BaseReader):
    files = [f for f in ["file_{}.ext".format(i) for i in range(10)]]
    tree = {
        "": {"root": {"path": {"project": {"": files}, "otherproject": {"": files}}}}
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
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        pass


@pytest.fixture
def job_manager():
    rw = ReaderWriter(reader=MockReader(), writer=MockWriter())
    session = make_session("sqlite://")
    job_manager = JobManager(session=session, reader_writer=rw)

    BaseModel.metadata.create_all(job_manager.session.bind.engine)

    yield job_manager

    BaseModel.metadata.drop_all(job_manager.session.bind.engine)

@pytest.fixture
def completed_job(job_manager):
    job = job_manager.init("file:///root/path/project/", "s3://bucket/project/")
    job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)

    yield job

@pytest.fixture
def partial_job(job_manager, completed_job):
    """simulate failed job with one item pending"""

    item = completed_job.items[0]

    job = completed_job
    item.status = ItemStatus.PENDING
    job.error = JobError.TRANSFER_ERROR
    job.last_state = JobStatus.PARSED
    job_manager.session.commit()

    yield job

