
from decouple import config

from . import make_session
from .batch_reader_writer import BatchReaderWriter
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import (AuthenticationError, InitDuplicateJobException,
                         InitSrcException, TransferException)
from .file import FileSystemReader
from .models import Item, Job
from .query import JobQueryArgs, Query
from .reader_writer import ReaderWriter
from .s3 import S3Writer
from .utils import _match_file_extension


class JobManager:
    def __init__(
        self,
        session,
        batch_reader_writer: BatchReaderWriter = BatchReaderWriter(),
    ):
        self.batch_reader_writer = batch_reader_writer
        self.session = session

    def run(self, job: Job):
        if job.last_state == JobStatus.DONE:
            return job

        try:
            self.batch_reader_writer.reader_writer.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            self.session.commit()
            raise AuthenticationError(message=e.message, operation=e.operation)

        self.batch_reader_writer.run(job)

        job.last_state = JobStatus.DONE
        self.session.commit()

        return job

    def init(self, source: str, destination: str, regexp: str = ".*"):
        job = Job(
            source=source,
            destination=destination,
            regexp=regexp,
        )

        job.error = JobError.NONE
        if not self._source_exists(source):
            raise InitSrcException(f"Source directory {source} not found.")

        duplicate_jobs = Query(self.session, Job).get(
            JobQueryArgs(source=source, destination=destination)
        )
        if duplicate_jobs:
            raise InitDuplicateJobException(
                f"Found duplicate job (id: {duplicate_jobs[0].id}) with source {source} and destination {destination}"
            )

        self.session.add(job)
        self.session.commit()

        return job

    def parse_and_commit_items(self, job: Job):
        if job.last_state > JobStatus.PARSED:
            return job

        # parse source
        in_uris = self._parse_source(
            job.source,
            files_only=True,
            pattern_filter=job.regexp,
            is_regex=True,
        )

        if job.destination[-1] == "/":
            # concatenate in_uri name
            out_uris = [job.destination + in_uri.split("/")[-1] for in_uri in in_uris]
        else:
            out_uris = [job.destination]

        items = [
            Item(
                in_uri=in_uri,
                out_uri=out_uri,
                status=ItemStatus.PENDING,
                job_id=job.id,
            )
            for in_uri, out_uri in zip(in_uris, out_uris)
        ]

        self.session.add_all(items)

        job.last_state = JobStatus.PARSED
        self.session.commit()

        return job

    def resume(self, job: Job):
        """Resume Job where status is not Done and file is Parsed"""

        if job.last_state < JobStatus.DONE:
            job.error = JobError.NONE
            job.info = None
            self.session.commit()
            self.run(job)
        else:
            print(f"Job {job.id} already done.")

        return job

    @classmethod
    def local_to_s3(cls, db_url: str = None, n_threads: int = 1):
        writer = S3Writer(profile_name=config("FORW_SERV_AWS_PROFILE_NAME", "default"))
        rw = ReaderWriter(reader=FileSystemReader(), writer=writer)

        session = make_session(db_url)
        job_manager = cls(session=session, reader_writer=rw, n_threads=n_threads)

        return job_manager



    def _source_exists(self, uri: str):
        return self.batch_reader_writer.reader_writer.reader.exists(uri)

    def _parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.batch_reader_writer.reader_writer.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_
