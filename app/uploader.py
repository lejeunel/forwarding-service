import io
import logging
import mimetypes
import os
from urllib.parse import urlparse
import fnmatch
import re
from multiprocessing import Pool, current_process
import copy
from aws_error_utils import get_aws_error_info

import boto3
from decouple import config
from abc import ABC, abstractmethod
from .auth import S3StaticCredentials, VaultCredentials, BaseAuthenticator
from botocore.client import ClientError as BotoClientError

from base64 import b64encode
import hashlib

logger = logging.getLogger("S3Sender")


def chunks(l, n):
    """Yield n number of striped chunks from l."""
    for i in range(0, n):
        yield l[i::n]


def _match_file_extension(filename: str, pattern: str, is_regex=False):
    """
    Function that return boolean, tell if the filename match the the given pattern

    filename -- (str) Name of file
    pattern -- (str) Pattern to filter file, can be '*.txt' if not regex, else '.*\txt'
    is_regex -- Bool If pattern is a regex or not
    """
    if not is_regex:
        pattern = fnmatch.translate(pattern)

    reobj = re.compile(pattern)
    match = reobj.match(filename)

    if match is None:
        return False
    return True


class BaseReader(ABC):
    @abstractmethod
    def read(self, uri):
        pass

    @abstractmethod
    def exists(self, uri):
        pass

    @staticmethod
    def guess_mime_type(uri):
        return mimetypes.guess_type(uri)[0]

    def __call__(self, uri):
        bytes_ = self.read(uri)
        type_ = self.guess_mime_type(uri)

        return bytes_, type_

    def refresh_credentials(self):
        pass

    def ping(self):
        pass


class BaseWriter(ABC):
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        pass

    def ping(self):
        pass


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

    def run(self, item_id, job_id):
        from .models import db, Item, Job
        from .enum_types import ItemStatus, JobError

        try:

            item = Item.query.get(item_id)
            job = Job.query.get(job_id)

            in_uri = item.uri
            out_uri = job.destination + in_uri.split('/')[-1]
            print(f'[{current_process().pid}] {in_uri} -> {out_uri}', flush=True)
            bytes_, type_ = self.reader(item.uri)

            checksum = None
            if self.do_checksum:
                checksum = self.compute_sha256_checksum(bytes_)

            self.writer(bytes_, out_uri, type_, checksum)

            item.status = ItemStatus.TRANSFERRED
            db.session.commit()

        except BotoClientError as e:
            err_info = get_aws_error_info(e)
            job.error = JobError.S3_ERROR
            job.info = {"message": err_info.message,
                        "operation": err_info.operation_name}
            db.session.commit()

    def run_parallel(self, items_id, job_id):
        list_in_out_uri = [(id, job_id) for id in items_id]

        with Pool(self.n_procs) as p:
            p.starmap(self.run, list_in_out_uri)

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

    def ping(self):
        """
        Checks that both source and destination are available
        """
        self.reader.ping()
        self.writer.ping()


class FileSystemReader(BaseReader):
    def read(self, uri: str):
        uri = urlparse(uri)
        with open(uri.path, "rb") as f:
            fileobj = io.BytesIO(f.read())

        return fileobj

    def exists(self, uri):
        return os.path.exists(urlparse(uri).path)

    def list(self, uri, files_only=True):
        """
        Return URIs of all items present at path
        """
        path = urlparse(uri).path
        if os.path.isfile(path):
            return [path]

        list_ = [path + f for f in os.listdir(path)]
        list_ = ['file://' +
                 f for f in list_ if(os.path.isfile(f) or not files_only)]

        return list_


class S3Writer(BaseWriter):
    def __init__(self, authenticator=BaseAuthenticator):
        self.client = boto3.client('s3')
        self.authenticator = authenticator

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k != 'client':
                setattr(result, k, copy.deepcopy(v, memo))
        setattr(result, 'client', boto3.client('s3'))

    def __reduce__(self):
        return (self.__class__, (self.authenticator,))

    def __call__(
            self, bytes_, uri, mime_type=None, checksum=None,
    ):
        uri = urlparse(uri)

        return self.client.put_object(
            Body=bytes_, Bucket=uri.netloc, Key=uri.path, ContentType=mime_type,
            ChecksumAlgorithm='SHA256',
            ChecksumSHA256=checksum
        )

    def refresh_credentials(self):
        creds = self.authenticator()
        self.client = boto3.client("s3", **creds)


class UploaderExtension(Uploader):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):

        source = app.config.get('UPLOADER_SOURCE')
        destination = app.config.get('UPLOADER_DESTINATION')
        auth_mode = app.config.get('UPLOADER_AUTH_MODE', None)
        auth_profile = app.config.get('UPLOADER_AUTH_PROFILE', 'default')

        if source == 'filesystem':
            self.reader = FileSystemReader()
        else:
            raise NotImplementedError

        # TODO this should be done at runtime, i.e. after job init
        if destination == 's3':
            authenticator = S3StaticCredentials()
            if auth_mode == 'env':
                creds = {'aws_access_key_id': config("AWS_ACCESS_KEY_ID"),
                         'aws_secret_access_key': config("AWS_SECRET_ACCESS_KEY")}
                authenticator = S3StaticCredentials(**creds)
            elif auth_mode == 'profile':
                authenticator = S3StaticCredentials()
            elif auth_mode == 'vault':
                authenticator = VaultCredentials(config('VAULT_URL'), config('VAULT_TOKEN_PATH'),
                                                 config('VAULT_ROLE_ID'), config('VAULT_SECRET_ID'))

            self.writer = S3Writer(authenticator)
            self.do_checksum = app.config.get('UPLOADER_CHECKSUM', False)
            self.n_procs = app.config.get('UPLOADER_N_PROCS', 1)
        else:
            raise NotImplementedError
