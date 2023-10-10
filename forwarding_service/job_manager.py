from multiprocessing import Pool

from decouple import config
from datetime import datetime

from . import make_session
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import (
    AuthenticationError,
    InitSrcError,
    InitDuplicateJobError,
    TransferError,
)
from .file import FileSystemReader
from .models import Item, Job
from .query import JobQueryArgs, Query
from .reader_writer import BaseReaderWriter, ReaderWriter
from .s3 import S3Writer
from .utils import _match_file_extension


class JobManager:
    def __init__(
        self,
        session,
        reader_writer: BaseReaderWriter = BaseReaderWriter(),
        n_procs: int = 1,
    ):
        self.reader_writer = reader_writer
        self.n_procs = n_procs
        self.session = session

    def run_parallel(self, items: list[Item]):
        in_out_uris = [(i.in_uri, i.out_uri) for i in items]
        with Pool(self.n_procs) as p:
            p.starmap(self.reader_writer.send, in_out_uris)

        job = self.session.get(Job, items[0].job_id)
        job.last_state = JobStatus.DONE

        for item in job.items:
            item.status = ItemStatus.TRANSFERRED
            item.transferred_at = datetime.now()

        self.session.commit()

        return job

    def run(self, job: Job):
        if job.last_state == JobStatus.DONE:
            return job

        try:
            self.reader_writer.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            self.session.commit()
            raise AuthenticationError(message=e.message, operation=e.operation)

        # filter-out items that are already transferred (happens on resume)
        items = (
            self.session.query(Item)
            .where(Item.job_id == job.id)
            .where(Item.status != ItemStatus.TRANSFERRED)
        ).all()

        if self.n_procs > 1:
            self.n_procs = min(self.n_procs, len(items))
            job = self.run_parallel(items)
        else:
            for item in items:
                try:
                    self.reader_writer.send(item.in_uri, item.out_uri)
                    item.transferred_at = datetime.now()
                    item.status = ItemStatus.TRANSFERRED
                    self.session.commit()
                except TransferError as e:
                    job.error = JobError.TRANSFER_ERROR
                    job.info = e.message
                    self.session.commit()
                    raise e

            job.last_state = JobStatus.DONE
            self.session.commit()

        return job

    def source_exists(self, uri: str):
        return self.reader_writer.reader.exists(uri)

    def _parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.reader_writer.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_

    def init(self, source: str, destination: str, regexp: str = ".*"):
        job = Job(
            source=source,
            destination=destination,
            regexp=regexp,
        )

        job.error = JobError.NONE
        if not self.source_exists(source):
            raise InitSrcError(f"Source directory {source} not found.")

        duplicate_jobs = Query(self.session, Job).get(
            JobQueryArgs(source=source, destination=destination)
        )
        if duplicate_jobs:
            raise InitDuplicateJobError(
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
    def local_to_s3(cls, db_url: str = None, aws_profile: str = None, n_procs: int = 1):
        if aws_profile is None:
            aws_profile = config("FORW_SERV_AWS_PROFILE_NAME", "default")
        writer = S3Writer(profile_name=aws_profile)
        rw = ReaderWriter(reader=FileSystemReader(), writer=writer)

        session = make_session(db_url)
        job_manager = cls(session=session, reader_writer=rw, n_procs=n_procs)

        return job_manager
