import hashlib
from base64 import b64encode
from datetime import datetime
from multiprocessing import Pool, current_process

from .base import BaseReader, BaseWriter
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import AuthenticationError, TransferError
from .models import Base
from .utils import _match_file_extension


class TransferAgent:
    def __init__(
        self,
        dbsession,
        reader: BaseReader,
        writer: BaseWriter,
        do_checksum=True,
        n_procs=1,
    ):
        self.reader = reader
        self.writer = writer
        self.do_checksum = do_checksum
        self.n_procs = n_procs
        self.dbsession = dbsession

        # this allows active record in this context via mixins
        # https://github.com/absent1706/sqlalchemy-mixins/tree/master#active-record
        Base.set_session(dbsession)

    @staticmethod
    def compute_sha256_checksum(bytes_):
        checksum = hashlib.sha256(bytes_.getbuffer())
        checksum = b64encode(checksum.digest()).decode()
        return checksum

    def upload_one_item(self, item_id: str):
        """
        Upload a single item.

        NOTE: As this function must work across parallel processes, we return
        deserialized objects and update the database from main process. This is because
        SQLite does not allow write-concurrency, i.e. several processes attempting to
        write simultaneously to the same DB.
        """
        from .enum_types import ItemStatus, JobError
        from .models import Item, Job

        try:
            item = Item.find(item_id)
            job = Job.find(item.job_id)

            in_uri = item.in_uri
            out_uri = item.out_uri

            print(f"[{current_process().pid}] {in_uri} -> {out_uri}")
            bytes_, type_ = self.reader(in_uri)

            checksum = None
            if self.do_checksum:
                checksum = self.compute_sha256_checksum(bytes_)

            self.writer(bytes_, out_uri, type_, checksum)

        except TransferError as e:
            item.status = ItemStatus.PENDING
            job.error = JobError.TRANSFER_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            return {"item": item.to_dict(), "job": job.to_dict()}

        item.status = ItemStatus.TRANSFERRED
        item.transferred = datetime.now()
        job.error = JobError.NONE
        return {"item": item.to_dict(), "job": job.to_dict()}

    def upload_parallel(self, items_id: list[str]):
        from .models import Item, Job

        with Pool(self.n_procs) as p:
            results = p.map(self.upload_one_item, items_id)

        job = sorted([r["job"] for r in results], key=lambda r: r["error"])[-1]
        job = JobSchema().load(results[0]["job"])
        items = ItemSchema(many=True).load([r["item"] for r in results])

        self.dbsession.bulk_update_mappings(Item, items)
        self.dbsession.query(Job).filter(Job.id == job["id"]).update(job)
        self.dbsession.commit()

        return job

    def upload(self, job_id: str):
        from .models import Item, Job

        job = Job.find(job_id)

        try:
            self.refresh_credentials()
        except AuthenticationError as e:
            job.update(
                error=JobError.AUTH_ERROR,
                info={"message": e.message, "operation": e.operation},
            )
            return job

        job.update(last_state=JobStatus.TRANSFERRING)

        # filter-out items that are already transferred (happens on resume)
        items = (
            self.dbsession.query(Item)
            .where(Item.job_id == job.id)
            .where(Item.status != ItemStatus.TRANSFERRED)
        )
        items_id = [i.id for i in items]

        if self.n_procs > 1:
            self.n_procs = min(self.n_procs, len(items_id))
            job = self.upload_parallel(items_id)
        else:
            for item_id in items_id:
                res = self.upload_one_item(item_id)

                item = Item.find(res["item"].pop("id"))
                job = Job.find(res["job"].pop("id"))
                item.update(**res["item"])
                job.update(**res["job"])

        if job.error == JobError.NONE:
            job.update(last_state=JobStatus.DONE)

        return job

    def src_exists(self, uri: str):
        return self.reader.exists(uri)

    def parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_

    def refresh_credentials(self):
        self.reader.refresh_credentials()
        self.writer.refresh_credentials()

    def init_job(self, source: str, destination: str, regexp: str = ".*"):
        from .enum_types import JobError
        from .models import Job

        job = Job(
            source=source,
            destination=destination,
            regexp=regexp,
        )

        job.error = JobError.NONE
        init_error = False
        messages = []
        if not self.src_exists(source):
            init_error = True
            messages.append(f"Source directory {source} not found.")

        if init_error:
            job.error = JobError.INIT_ERROR
            job.info = {"message": messages}
            print(job.error, messages)

        self.dbsession.add(job)
        self.dbsession.commit()

        return job

    def parse_and_commit_items(self, job_id):
        """Parse items from given job_id
        Save in database all parsed items, information such as location are retrieved from
        from job description

        Args:
            job_id (str): Job id

        Returns:
            Item: return all parsed items
        """
        from .models import Item, Job

        job = self.dbsession.get(Job, job_id)
        job.last_state = JobStatus.PARSING
        self.dbsession.commit()

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
                created=datetime.now(),
            )
            for in_uri, out_uri in zip(in_uris, out_uris)
        ]

        self.dbsession.add_all(items)
        self.dbsession.commit()

        job.last_state = JobStatus.PARSED
        self.dbsession.commit()

        return self.dbsession.query(Item).filter(Item.job_id == job.id).all()

    def resume(self, job_id: str):
        """Resume Job where status is not Done and file is Parsed

        Args:
            redis_url (str): Redis url
            job_id (str): job id to resume
        """
        from .models import Job

        job = Job.find(job_id)

        if job.last_state < JobStatus.DONE:
            job.update(error=JobError.NONE, info=None)
            self.upload(job.id)
        else:
            print(f"Job {job_id} already done.")

        return job
