import hashlib
import os
from datetime import datetime
from pathlib import Path

from aws_error_utils import get_aws_error_info
from botocore.client import ClientError as BotoClientError
from hvac.exceptions import VaultError
from sqlalchemy.exc import IntegrityError

from . import rq
from .utils import MD5CheckSumException
from base64 import b64encode


def _parse_files(job_id):
    """Parse files from given job_id
    Save in database all parsed files, information such as location are retrieved from
    from job description

    Args:
        job_id (str): Job id

    Returns:
        File: return all files parsed
    """
    from . import db, fs
    from .enum_types import FileStatus, JobStatus
    from .models import File, Job

    job = db.session.get(Job, job_id)
    job.last_state = JobStatus.PARSING
    db.session.commit()

    # parse directory
    in_files = fs.listdir(
        job.source_path,
        files_only=True,
        pattern_filter=job.regexp,
        is_regex=True,
    )

    # when we have a single item, that means its a single file
    if len(in_files) == 1:
        job.source_path = str(Path(job.source_path).parent)

    for f in in_files:
        f = File(
            filename=f.split("/")[-1],
            status=FileStatus.PENDING,
            job_id=str(job.id),
            creation_date=datetime.now(),
        )

        # check for unique constraint (raise IntegrityError)
        # this should keep existing record
        try:
            db.session.add(f)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()

    job.last_state = JobStatus.PARSED
    db.session.commit()

    return db.session.query(File).filter(File.job_id == job.id).all()


@rq.job
def _upload_job(job_id):
    """Launch an existing job from job_id

    Args:
        job_id (str): job id

    Raises:
        BotoClientError: return exception when error occur with aws part
        MD5CheckSumException: return exception when error occur with corrupted file
        VaultError: return exception when error occur with auth
        None
    """
    from . import db, s3
    from .enum_types import FileStatus, JobError, JobStatus
    from .models import Job, File

    job = db.session.get(Job, job_id)
    if job.last_state != JobStatus.PARSED:

        files = _parse_files(job_id)
    else:
        files = db.session.query(File).filter(File.job_id == job.id).all()

    # filter-out files that are already transferred (happens on resume)
    files = [f for f in files if f.status != FileStatus.TRANSFERRED]
    bucket = job.bucket

    try:
        # fetch credentials from vault and configure s3 client
        s3.refresh_credentials()
        s3.client.head_bucket(Bucket=bucket)

        job.last_state = JobStatus.TRANSFERRING
        # TODO add multiprocessing pool here
        for f in files:

            # load to bytes and guess mimetypes
            fileobj, extra_args = s3.load_fileobj(
                os.path.join(job.source_path, f.filename)
            )

            # compute checksum
            checksum = hashlib.sha256(fileobj.getbuffer())
            checksum = b64encode(checksum.digest()).decode()

            key = os.path.join(job.prefix, f.filename)

            # upload file
            res = s3.client.put_object(
                Body=fileobj.getvalue(),
                Bucket=bucket,
                Key=key,
                ContentType=extra_args['ContentType'],
                ChecksumAlgorithm='SHA256',
                ChecksumSHA256=checksum)

            f.status = FileStatus.TRANSFERRED

            db.session.commit()
    except BotoClientError as e:
        err_info = get_aws_error_info(e)
        job.error = JobError.S3_ERROR
        job.info = {"message": err_info.message,
                    "operation": err_info.operation_name}
    except MD5CheckSumException:
        f.status = FileStatus.ERROR
        job.error = JobError.CHECKSUM_ERROR
        job.info = {"message": f"Detected corruption transferring {f.filename}"}
    except VaultError as e:
        job.error = JobError.VAULT_ERROR
        job.info = {"message": e.errors, "operation": ""}

    if job.error == JobError.NONE:
        job.last_state = JobStatus.DONE
    db.session.commit()

    return


def _init_job(bucket, prefix, testing, regexp, src, user="GENERIC"):
    """Initialization of job.
    We perform basic checks prior to queueing up.

    Args:
        bucket (str): Bucket destination for s3
        prefix (str): Prefix for s3 location
        testing (bool): _description_
        regexp (str): Regular Expression for filtering file
        src (str): Source directory where file are stored
        user (str, optional): _description_. Defaults to "GENERIC".

    Returns:
        Job: return the generated job
    """
    from . import db, fs
    from .enum_types import JobError
    from .models import Job
    from .utils import user_bucket_allowed, user_bucket_dirs_allowed

    if not prefix.endswith("/"):
        prefix += "/"

    job = Job(
        bucket=bucket,
        prefix=prefix,
        source_path=src,
        regexp=regexp,
        user=user,
    )

    job.error = JobError.NONE
    init_error = False
    messages = []
    if not fs.path_exists(src):
        init_error = True
        messages.append(f"Source directory {src} not found.")
    if not fs.is_allowed(src):
        init_error = True
        messages.append(f"Source directory {src} not allowed.")
    if not user_bucket_allowed(user, bucket):
        init_error = True
        messages.append(f"User {user} not allowed to bucket {bucket}.")
    if not user_bucket_dirs_allowed(user, bucket, src):
        init_error = True
        messages.append(f"User {user} not allowed from root dir {src}.")

    if init_error:
        job.error = JobError.INIT_ERROR
        job.info = {"message": messages}

    db.session.add(job)
    db.session.commit()

    return job


def _upload(redis_url, bucket, prefix, testing, regexp, src, user="GENERIC"):
    """Main function that performs upload.

    Args:
        redis_url (_type_): _description_
        bucket (str): Bucket destination for s3
        prefix (str): Prefix for s3 location
        testing (bool): If true, job are launch sequentially and not enqued
        regexp (str): Regular Expression for filtering file
        src (str): Source directory where file are stored
        user (str, optional): _description_. Defaults to "GENERIC".
    """
    from . import db, fs, rq
    from .enum_types import JobError
    from .models import Job
    from .schemas import JobSchema
    from .worker import _upload_job
    from .utils import user_bucket_allowed

    job = _init_job(bucket, prefix, testing, regexp, src, user)
    if job.error != JobError.NONE:
        return

    if testing:
        _upload_job(job.id)
    else:
        rq.redis_url = redis_url
        default_queue = rq.get_queue()
        job = _upload_job.queue(job.id)


def _resume_job(redis_url, job_id, testing):
    """Resume Job where status is not Done and file is Parsed

    Args:
        redis_url (str): Redis url
        job_id (str): job id to resume
        testing (bool): If true, job are launch sequentially and not enqued
    """
    from rq import Queue
    from . import rq, db
    from .models import Job
    from .enum_types import JobStatus, JobError

    job = db.session.get(Job, job_id)

    if job.last_state != JobStatus.DONE:
        job.error = JobError.NONE
        job.info = None
        db.session.commit()
        if testing:
            _upload_job(job.id)
        else:
            rq.redis_url = redis_url
            default_queue = rq.get_queue()
            job = _upload_job.queue(job.id)
