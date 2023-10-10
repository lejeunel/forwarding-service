from multiprocessing import Pool
from uuid import UUID

from decouple import config
from pydantic import validate_call

from . import make_session
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import AuthenticationError, InitError
from .file import FileSystemReader
from .models import Item, Job
from .query import Query
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
        self.query = Query(session)

    def run_parallel(self, items: list[Item]):
        in_out_uris = [(i.in_uri, i.out_uri) for i in items]
        with Pool(self.n_procs) as p:
            results = p.starmap(self.reader_writer.send, in_out_uris)

        job_meta = sorted([r["job"] for r in results], key=lambda r: r["error"])[-1]
        job = self.session.get(Job, items[0].job_id)
        job.error = job_meta["error"]
        job.info = job_meta["info"]

        # TODO should be bulk update here
        for item, res in zip(items, results):
            item.status = res["item"]["status"]
            item.transferred_at = res["item"]["transferred_at"]

        self.session.commit()

        return job

    def run(self, job_id: UUID):
        job = self.session.get(Job, job_id)

        try:
            self.reader_writer.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            self.session.commit()
            return job

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
                res = self.reader_writer.send(item.in_uri, item.out_uri)

                if res['job']['error'] == JobError.NONE:
                    item.transferred_at = res["item"]["transferred_at"]
                item.status = res["item"]["status"]
                job.error = res["job"]["error"]
                job.info = res["job"]["info"]
                self.session.commit()

        if job.error == JobError.NONE:
            job.last_state = JobStatus.DONE
            self.session.commit()

        return job

    def source_exists(self, uri: str):
        return self.reader_writer.reader.exists(uri)

    def parse_source(
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
            raise InitError(f"Source directory {source} not found.")

        duplicate_jobs = self.query.jobs(source=source, destination=destination)
        if duplicate_jobs:
            raise InitError(f"Found duplicate job (id: {duplicate_jobs[0].id}) with source {source} and destination {destination}")

        self.session.add(job)
        self.session.commit()

        return job

    @validate_call
    def parse_and_commit_items(self, job_id: UUID):
        """Parse items from given job_id and save in database

        Args:
            job_id (str): Job id

        Returns:
            Item: return all parsed items
        """

        jobs = self.query.jobs(JobQueryArgs(id=job_id))
        if not jobs:
            raise Exception(f'Could not find job with id {job_id}')

        job = jobs[0]

        # parse source
        in_uris = self.parse_source(
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

    @validate_call
    def resume(self, job_id: UUID):
        """Resume Job where status is not Done and file is Parsed
        """

        jobs = self.query.jobs(JobQueryArgs(id=job_id))
        if not jobs:
            raise Exception(f'Could not find job with id {job_id}')

        job = jobs[0]

        if job.last_state < JobStatus.DONE:
            job.error = JobError.NONE
            job.info = None
            self.session.commit()
            self.run(job.id)
        else:
            print(f"Job {job_id} already done.")

        return job


    @validate_call
    def delete_job(self, job_id: UUID):
        jobs = self.query.jobs(JobQueryArgs(id=job_id))
        if not jobs:
            raise Exception(f'Could not find job with id {job_id}')

        job = jobs[0]

        self.session.delete(job)
        self.session.commit()


    @classmethod
    def local_to_s3(cls, db_url: str = None, n_procs: int = 1):

        writer = S3Writer(profile_name=config("FORW_SERV_AWS_PROFILE_NAME", 'default'))
        rw = ReaderWriter(reader=FileSystemReader(), writer=writer)

        session = make_session(db_url)
        job_manager = cls(session=session, reader_writer=rw, n_procs=n_procs)

        return job_manager

    @classmethod
    def viewer(cls, db_url: str=None):
        session = make_session(db_url)
        return cls(session=session)
