import hashlib
from base64 import b64encode
from datetime import datetime
from multiprocessing import Pool, current_process

from .base import BaseReader, BaseWriter
from .exceptions import AuthenticationError, TransferError
from .utils import _match_file_extension


class Uploader:
    def __init__(
        self, reader: BaseReader, writer: BaseWriter, do_checksum=True, n_procs=8
    ):
        self.reader = reader
        self.writer = writer
        self.do_checksum = do_checksum
        self.n_procs = n_procs

    @staticmethod
    def compute_sha256_checksum(bytes_):
        checksum = hashlib.sha256(bytes_.getbuffer())
        checksum = b64encode(checksum.digest()).decode()
        return checksum

    def upload_one_item(self, item_id):
        from .enum_types import ItemStatus, JobError
        from .models import Item, Job, db

        try:
            item = db.session.get(Item, item_id)

            in_uri = item.in_uri
            out_uri = item.in_uri

            print(f"[{current_process().pid}] {in_uri} -> {out_uri}")
            bytes_, type_ = self.reader(in_uri)

            checksum = None
            if self.do_checksum:
                checksum = self.compute_sha256_checksum(bytes_)

            self.writer(bytes_, out_uri, type_, checksum)

        except TransferError as e:
            info = {"message": e.message, "operation": e.operation}
            return {
                "item_id": item.id,
                "item_status": ItemStatus.PENDING,
                "item_transferred": None,
                "job_error": JobError.TRANSFER_ERROR,
                "job_info": info,
            }

        return {
            "item_id": item.id,
            "item_status": ItemStatus.TRANSFERRED,
            "item_transferred": datetime.now(),
            "job_error": JobError.NONE,
            "job_info": None,
        }

    def upload_parallel(self, items_id):
        from .models import Item, Job, db

        with Pool(self.n_procs) as p:
            results = p.map(self.upload_one_item, items_id)

        # find most critical job error and update db record
        job_error_info = sorted(
            [(r["job_error"], r["job_info"]) for r in results], key=lambda t: t[0]
        )[-1]
        item = db.session.get(Item, items_id[0])
        job = db.session.get(Job, item.job_id)
        job.error = job_error_info[0]
        job.info = job_error_info[1]

        # bulk update item status
        new_statii = [
            {
                "id": r["item_id"],
                "status": r["item_status"],
                "transferred": r["item_transferred"],
            }
            for r in results
        ]
        db.session.bulk_update_mappings(Item, new_statii)

        db.session.commit()

    def run_job(self, job_id):
        from . import db
        from .enum_types import ItemStatus, JobError, JobStatus
        from .models import Item, Job

        job = db.session.get(Job, job_id)

        try:
            self.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {"message": e.message, "operation": e.operation}
            db.session.commit()
            return

        job.last_state = JobStatus.TRANSFERRING

        # filter-out items that are already transferred (happens on resume)
        items = (
            db.session.query(Item)
            .where(Item.job_id == job.id)
            .where(Item.status != ItemStatus.TRANSFERRED)
        )
        items_id = [i.id for i in items]

        if self.n_procs > 1:
            self.n_procs = min(self.n_procs, len(items_id))
            self.upload_parallel(items_id)
        else:
            for item_id in items_id:
                res = self.upload_one_item(item_id)
                item = db.session.get(Item, res["item_id"])
                item.status = res["item_status"]
                item.transferred = res["item_transferred"]
                job.error = res["job_error"]
                job.info = res["job_info"]
                db.session.commit()

    def src_exists(self, uri):
        return self.reader.exists(uri)

    def src_list(self, uri, files_only=False, pattern_filter="*.*", is_regex=False):
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
