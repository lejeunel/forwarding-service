from datetime import datetime

from aws_error_utils import get_aws_error_info
from botocore.client import ClientError as BotoClientError
from hvac.exceptions import VaultError
from rich import print
from sqlalchemy.exc import IntegrityError


def _parse_and_commit_items(job_id):
    """Parse items from given job_id
    Save in database all parsed items, information such as location are retrieved from
    from job description

    Args:
        job_id (str): Job id

    Returns:
        Item: return all parsed items
    """
    from . import db, fs
    from .enum_types import ItemStatus, JobStatus
    from .models import Item, Job

    job = db.session.get(Job, job_id)
    job.last_state = JobStatus.PARSING
    db.session.commit()

    # parse source
    uris = fs.src_list(
        job.source,
        files_only=True,
        pattern_filter=job.regexp,
        is_regex=True,
    )

    for uri in uris:
        item = Item(
            uri=uri,
            status=ItemStatus.PENDING,
            job_id=str(job.id),
            creation_date=datetime.now(),
        )

        # check for unique constraint (raise IntegrityError)
        # this should keep existing record
        try:
            db.session.add(item)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()

    job.last_state = JobStatus.PARSED
    db.session.commit()

    return db.session.query(Item).filter(Item.job_id == job.id).all()


def _upload(job_id):
    """Run a job

    Args:
        job_id (str): job id

    Raises:
        BotoClientError: return exception when error occur with aws part
        MD5CheckSumException: return exception when error occur with corrupted file
        VaultError: return exception when error occur with auth
        None
    """
    from . import db, fs
    from .enum_types import JobError, JobStatus
    from .models import Job

    job = db.session.get(Job, job_id)
    if job.last_state != JobStatus.PARSED:

        _parse_and_commit_items(job_id)

    try:
        fs.setup(job.source, job.destination)
        fs.run_job(job_id)
    except BotoClientError as e:
        err_info = get_aws_error_info(e)
        job.error = JobError.S3_ERROR
        job.info = {"message": err_info.message,
                    "operation": err_info.operation_name}
    except VaultError as e:
        job.error = JobError.VAULT_ERROR
        job.info = {"message": e.errors, "operation": ""}

    if job.error == JobError.NONE:
        job.last_state = JobStatus.DONE
    db.session.commit()

    return job


def _init(source, destination, regexp):
    """Initialization of job.
    We perform basic checks prior to queueing up.

    Args:
        bucket (str): Bucket destination for s3
        prefix (str): Prefix for s3 location
        regexp (str): Regular Expression for filtering file
        src (str): Source directory where file are stored
        user (str, optional): _description_. Defaults to "GENERIC".

    Returns:
        Job: return the generated job
    """
    from . import db, fs
    from .enum_types import JobError
    from .models import Job

    job = Job(
        source=source,
        destination=destination,
        regexp=regexp,
    )

    fs.setup(source, destination)

    job.error = JobError.NONE
    init_error = False
    messages = []
    if not fs.src_exists(source):
        init_error = True
        messages.append(f"Source directory {source} not found.")

    if ((source[-1] != '/') or (destination[-1] != '/')):
        init_error = True
        messages.append(
            f"source {source} and destination {destination} must have trailing slashes.")

    if init_error:
        job.error = JobError.INIT_ERROR
        job.info = {"message": messages}
        print(job.error, messages)

    db.session.add(job)
    db.session.commit()

    return job


def _init_and_upload(source, destination, regexp='.*'):
    """Main function that performs upload.

    Args:
        bucket (str): Bucket destination for s3
        prefix (str): Prefix for s3 location
        regexp (str): Regular Expression for filtering file
        src (str): Source directory where file are stored
        user (str, optional): _description_. Defaults to "GENERIC".
    """
    from .enum_types import JobError

    job = _init(source, destination, regexp)
    if job.error != JobError.NONE:
        return job

    return _upload(job.id)


def _resume(job_id):
    """Resume Job where status is not Done and file is Parsed

    Args:
        redis_url (str): Redis url
        job_id (str): job id to resume
    """
    from . import db
    from .enum_types import JobError, JobStatus
    from .models import Job

    job = db.session.get(Job, job_id)

    if job.last_state != JobStatus.DONE:
        job.error = JobError.NONE
        job.info = None
        db.session.commit()
        _upload_job(job.id)
    else:
        print(f'Job {job_id} already done.')
