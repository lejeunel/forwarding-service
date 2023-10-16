from decouple import config
from pydantic import ValidationError

from . import make_session
from .batch_reader_writer import BatchReaderWriter
from .commands import (
    RaiseJobExceptionCommand,
    HandleExceptionCommand,
    UpdateItemStatusCommand,
    UpdateJobDoneCommand,
    CommitChangesCommand,
)
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import (
    AuthenticationError,
    InitDuplicateJobException,
    InitException,
    InitSrcException,
)
from .file import FileSystemReader
from .models import Item, Job
from .query import JobQueryArgs, Query
from .s3 import S3Writer
from .utils import _match_file_extension


class JobManager:
    def __init__(
        self,
        session,
        batch_reader_writer: BatchReaderWriter,
    ):
        self.session = session
        self.batch_rw = batch_reader_writer

        self.batch_rw.register_post_batch_command(UpdateJobDoneCommand())
        self.batch_rw.register_post_batch_command(
            CommitChangesCommand(self.session)
        )
        self.batch_rw.register_post_batch_command(RaiseJobExceptionCommand())

        self.batch_rw.register_post_item_command(UpdateItemStatusCommand())
        self.batch_rw.register_post_item_command(HandleExceptionCommand())

        # Multi-threading imposes some limitations:
        # - Concurrent threads (usually) cannot write to the same database
        # - Exceptions are silently ignored when raised inside a thread
        if self.batch_rw.n_threads == 1:
            self.batch_rw.register_post_item_command(
                CommitChangesCommand(self.session)
            )
            self.batch_rw.register_post_item_command(RaiseJobExceptionCommand())

    def run(self, job: Job):
        if job.status == JobStatus.DONE:
            return job

        try:
            self.batch_rw.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.error, "operation": e.operation}
            self.session.commit()
            raise AuthenticationError(error=e.error, operation=e.operation)

        items = [
            item for item in job.items if item.status != ItemStatus.TRANSFERRED
        ]

        self.batch_rw.run(items)

        job.status = JobStatus.DONE
        self.session.commit()

        return job

    def init(self, source: str, destination: str, regexp: str = ".*"):
        try:
            job = Job.validate(
                {"source": source, "destination": destination, "regexp": regexp}
            )
        except ValidationError as e:
            raise InitException(e.errors)

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
        if job.status > JobStatus.PARSED:
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
            out_uris = [
                job.destination + in_uri.split("/")[-1] for in_uri in in_uris
            ]
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

        job.status = JobStatus.PARSED
        self.session.commit()

        return job

    def resume(self, job: Job):
        """Resume Job where status is not Done and file is Parsed"""

        if job.status < JobStatus.DONE:
            job.error = JobError.NONE
            job.info = None
            self.session.commit()
            self.run(job)
        else:
            print(f"Job {job.id} already done.")

        return job

    @classmethod
    def local_to_s3(
        cls, db_url: str = None, n_threads=30, split_ratio: float = 0.1
    ):
        writer = S3Writer(
            profile_name=config("FORW_SERV_AWS_PROFILE_NAME", "default")
        )
        brw = BatchReaderWriter(
            reader=FileSystemReader(),
            writer=writer,
            n_threads=n_threads,
            split_ratio=split_ratio,
        )

        session = make_session(db_url)
        job_manager = cls(session=session, batch_reader_writer=brw)

        return job_manager

    def _source_exists(self, uri: str):
        return self.batch_rw.reader.exists(uri)

    def _parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.batch_rw.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_
