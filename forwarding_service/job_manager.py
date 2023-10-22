from decouple import config
from pydantic import ValidationError

from . import make_session
from .commands import (
    RaiseExceptionCommand,
    UpdateItemStatusCommand,
    UpdateJobErrorCommand,
)
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import (
    AuthenticationError,
    InitDuplicateJobException,
    InitException,
    InitSrcException,
)
from .models import Item, Job, Transaction
from .query import JobQueryArgs, Query
from .transfer_agent import TransferAgent
from .utils import _match_file_extension


class JobManager:
    def __init__(
        self,
        session,
        transfer_agent: TransferAgent,
    ):
        self.session = session
        self.transfer_agent = transfer_agent

    def run(self, job: Job):
        if job.status == JobStatus.DONE:
            return job

        self._refresh_credentials(job)

        items = [
            item for item in job.items if item.status != ItemStatus.TRANSFERRED
        ]

        self._setup_commands()
        transactions = [
            Transaction(item_id=i.id, input=i.in_uri, output=i.out_uri)
            for i in items
        ]
        self.transfer_agent.run(transactions)

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

        self._refresh_credentials(job)

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

    def _source_exists(self, uri: str):
        return self.transfer_agent.reader.exists(uri)

    def _parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.transfer_agent.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_

    def _setup_commands(self):
        """Assign commands to underlying transfer agent.

        Depending on regime (sequential, threaded),
        some commands will be skipped as threads cannot handle
        committing to database and raising exceptions
        """
        threaded = self.transfer_agent.n_threads > 1

        self.transfer_agent.post_batch_commands = [
            UpdateItemStatusCommand(self.session),
            UpdateJobErrorCommand(self.session),
            RaiseExceptionCommand(),
        ]

        self.transfer_agent.post_transaction_commands = [
            UpdateItemStatusCommand(self.session, threaded),
            UpdateJobErrorCommand(self.session, threaded),
            RaiseExceptionCommand(threaded),
        ]

    def _refresh_credentials(self, job):
        try:
            self.transfer_agent.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.error, "operation": e.operation}
            self.session.commit()
            raise AuthenticationError(error=e.error, operation=e.operation)

    @classmethod
    def local_to_s3(
        cls, db_url: str = None, n_threads=30, split_ratio: float = 0.1
    ):
        from .file import FileSystemReader
        from .s3 import S3Writer

        writer = S3Writer(
            profile_name=config("FORW_SERV_AWS_PROFILE_NAME", "default")
        )
        agent = TransferAgent(
            reader=FileSystemReader(),
            writer=writer,
            n_threads=n_threads,
            split_ratio=split_ratio,
        )

        session = make_session(db_url)
        job_manager = cls(session=session, transfer_agent=agent)

        return job_manager
