from datetime import datetime
from multiprocessing import Pool
from pathlib import Path

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from .base import BaseReader
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import AuthenticationError
from .item_uploader import ItemUploader
from .utils import _match_file_extension
from .models import Item


class TransferAgent:
    def __init__(
        self,
        uploader: ItemUploader,
        db_url: str,
        n_procs=1,
    ):
        self.uploader = uploader
        self.n_procs = n_procs
        self.db_url = db_url
        self.session = self.make_session(self.db_url)

    @staticmethod
    def make_session(db_url):
        if db_url is None:
            db_path = Path(
                config("FORW_SERV_DB_PATH", "~/.cache/forwarding_service.db")
            ).expanduser()
            assert db_path.parent.exists(), f"{db_path.parent} not found."

        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        return session

    def upload_parallel(self, items: list[Item]):
        from .models import Item, Job


        in_out_uris = [(i.in_uri, i.out_uri) for i in items]
        with Pool(self.n_procs) as p:
            results = p.starmap(self.uploader.upload, in_out_uris)

        job_meta = sorted([r["job"] for r in results], key=lambda r: r["error"])[-1]
        job = self.session.get(Job, items[0].job_id)
        job.error = job_meta['error']
        job.info = job_meta['info']

        # TODO should be bulk update here
        for item, res in zip(items, results):
            item.status = res['item']['status']
            item.transferred = res['item']['transferred']


        self.session.commit()

        return job

    def upload(self, job_id: str):
        from .models import Item, Job

        job = self.session.get(Job, job_id)

        try:
            self.uploader.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            self.session.commit()
            return job

        job.last_state = JobStatus.TRANSFERRING
        self.session.commit()

        # filter-out items that are already transferred (happens on resume)
        items = (
            self.session.query(Item)
            .where(Item.job_id == job.id)
            .where(Item.status != ItemStatus.TRANSFERRED)
        )

        if self.n_procs > 1:
            self.n_procs = min(self.n_procs, len(items.all()))
            job = self.upload_parallel(items.all())
        else:
            for item in items:
                res = self.uploader.upload(item.in_uri, item.out_uri)

                job = self.session.get(Job, item.job_id)
                item.status = res["item"]["status"]
                item.transferred = res["item"]["transferred"]
                job.error = res["job"]["error"]
                job.info = res["job"]["info"]
                self.session.add(item)
                self.session.add(job)
                self.session.commit()

        if job.error == JobError.NONE:
            job.last_state = JobStatus.DONE
            self.session.commit()

        return job

    def src_exists(self, uri: str):
        return self.uploader.reader.exists(uri)

    def parse_source(
        self,
        uri: str,
        files_only: bool = False,
        pattern_filter: str = "*.*",
        is_regex: bool = False,
    ):
        list_ = self.uploader.reader.list(uri, files_only=files_only)

        list_ = [
            item
            for item in list_
            if _match_file_extension(item, pattern_filter, is_regex)
        ]
        return list_

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

        self.session.add(job)
        self.session.commit()

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

        job = self.session.get(Job, job_id)
        job.last_state = JobStatus.PARSING
        self.session.commit()

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

        self.session.add_all(items)

        job.last_state = JobStatus.PARSED
        self.session.commit()

        return self.session.query(Item).filter(Item.job_id == job.id).all()

    def resume(self, job_id: str):
        """Resume Job where status is not Done and file is Parsed

        Args:
            redis_url (str): Redis url
            job_id (str): job id to resume
        """
        from .models import Job

        job = self.session.get(Job, job_id)

        if job.last_state < JobStatus.DONE:
            job.error = error = JobError.NONE
            job.info = None
            self.session.commit()
            self.upload(job.id)
        else:
            print(f"Job {job_id} already done.")

        return job


if __name__ == "__main__":
    agent = TransferAgent(Session(), BaseReader())
