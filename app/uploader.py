import hashlib
import logging
from base64 import b64encode
from multiprocessing import Pool, current_process
from urllib.parse import urlparse

from decouple import config
from .exceptions import TransferError, AuthenticationError

from .auth import S3StaticCredentials, VaultCredentials
from .base import BaseReader, BaseWriter
from .file import FileSystemReader
from .s3 import S3Writer
from .utils import _match_file_extension

logger = logging.getLogger("S3Sender")


class Uploader:
    def __init__(self, reader: BaseReader, writer: BaseWriter, do_checksum=True, n_procs=8):
        self.reader = reader
        self.writer = writer
        self.do_checksum = do_checksum
        self.n_procs = n_procs

    @staticmethod
    def compute_sha256_checksum(bytes_):
        # compute checksum
        checksum = hashlib.sha256(bytes_.getbuffer())
        checksum = b64encode(checksum.digest()).decode()
        return checksum

    def upload_one_item(self, item_id, job_id):
        from .enum_types import ItemStatus, JobError
        from .models import Item, Job, db

        try:

            item = db.session.get(Item, item_id)
            job = db.session.get(Job, job_id)

            in_uri = item.uri
            out_uri = job.destination + in_uri.split('/')[-1]
            print(f'[{current_process().pid}] {in_uri} -> {out_uri}')
            bytes_, type_ = self.reader(item.uri)

            checksum = None
            if self.do_checksum:
                checksum = self.compute_sha256_checksum(bytes_)

            self.writer(bytes_, out_uri, type_, checksum)

            item.status = ItemStatus.TRANSFERRED

        except TransferError as e:
            info = {"message": e.message,
                    "operation": e.operation}
            return {'item_id': item.id, 'item_status': item.status, 'job_error': JobError.TRANSFER_ERROR, 'job_info': info}
        return {'item_id': item.id, 'item_status': item.status, 'job_error': JobError.NONE, 'job_info': None}

    def run_parallel(self, items_id, job_id):
        from .models import db, Job, Item

        list_in_out_uri = [(id, job_id) for id in items_id]

        with Pool(self.n_procs) as p:
            results = p.starmap(self.upload_one_item, list_in_out_uri)

        # find most critical job error and update db record
        job_error_info = sorted([(r['job_error'], r['job_info'])
                                 for r in results], key=lambda t: t[0])[-1]
        job = db.session.get(Job, job_id)
        job.error = job_error_info[0]
        job.info = job_error_info[1]

        # bulk update item status
        new_statii = [{'id': r['item_id'], 'status': r['item_status']}
                      for r in results]
        db.session.bulk_update_mappings(Item, new_statii)

        db.session.commit()

    def run_job(self, job_id):
        from . import db
        from .enum_types import ItemStatus, JobStatus, JobError
        from .models import Item, Job

        job = db.session.get(Job, job_id)

        try:
            self.refresh_credentials()
        except AuthenticationError as e:
            job.error = JobError.AUTH_ERROR
            job.info = {'message': e.message, 'operation': e.operation}
            db.session.commit()
            return

        job.last_state = JobStatus.TRANSFERRING

        # filter-out items that are already transferred (happens on resume)
        items = db.session.query(Item).where(Item.job_id == job.id).where(
            Item.status != ItemStatus.TRANSFERRED)
        items_id = [i.id for i in items]

        if self.n_procs > 1:
            self.run_parallel(items_id, job_id)
        else:
            for item_id in items_id:
                res = self.upload_one_item(item_id, job_id)
                item = db.session.get(Item, res['item_id'])
                item.status = res['item_status']
                job.error = res['job_error']
                job.info = res['job_info']
                db.session.commit()

    def src_exists(self, uri):
        return self.reader.exists(uri)

    def src_list(self, uri, files_only=False, pattern_filter="*.*", is_regex=False):
        list_ = self.reader.list(uri, files_only=files_only)

        list_ = [item for item in list_ if _match_file_extension(
            item, pattern_filter, is_regex)]
        return list_

    def refresh_credentials(self):
        self.reader.refresh_credentials()
        self.writer.refresh_credentials()


class UploaderExtension(Uploader):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):

        self.source = app.config.get('UPLOADER_SOURCE')
        self.destination = app.config.get('UPLOADER_DESTINATION')
        self.auth_mode = app.config.get('UPLOADER_AUTH_MODE', None)
        self.do_checksum = app.config.get('UPLOADER_CHECKSUM', False)
        self.n_procs = app.config.get('UPLOADER_N_PROCS', 1)

    def setup(self, source, destination):
        """
        Setup reader and writer given URIs. This should be called before run.
        """
        from . import db
        from .models import Job

        in_scheme = urlparse(source).scheme
        out_scheme = urlparse(destination).scheme

        if in_scheme == 'file':
            self.reader = FileSystemReader()
        else:
            raise NotImplementedError

        if out_scheme == 's3':
            self.authenticator = S3StaticCredentials()
            if self.auth_mode == 'env':
                creds = {'aws_access_key_id': config("AWS_ACCESS_KEY_ID"),
                         'aws_secret_access_key': config("AWS_SECRET_ACCESS_KEY")}
                self.authenticator = S3StaticCredentials(**creds)
            elif self.auth_mode == 'profile':
                self.authenticator = S3StaticCredentials()
            elif self.auth_mode == 'vault':
                self.authenticator = VaultCredentials(config('VAULT_URL'), config('VAULT_TOKEN_PATH'),
                                                      config('VAULT_ROLE_ID'), config('VAULT_SECRET_ID'))

            self.writer = S3Writer(self.authenticator)
        else:
            raise NotImplementedError
