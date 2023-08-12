import logging

import click

from .worker import _upload, _resume_job
from rich import print

logging.basicConfig(level=logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


@click.command("upload", help="Upload data command")
@click.option(
    "--redis-url", help="Redis server url", default="redis://localhost:6379/0"
)
@click.option("--bucket", type=str, default="bucket")
@click.option("--prefix", type=str, default="prefix", help="Prefix to key(s)")
@click.option("--testing/--no-testing", default=False, help="Launch job sequentially")
@click.option("--regexp", default=".*", help="Regular expression")
@click.option("--user", default="GENERIC", help="User name")
@click.argument("src", type=str)
def upload(redis_url, bucket, prefix, testing, regexp, user, src):
    return _upload(redis_url, bucket, prefix, testing, regexp, src, user)


@click.command("resume-job")
@click.option(
    "--redis-url", help="Redis server url", default="redis://localhost:6379/0"
)
@click.option("--testing/--no-testing", default=False, help="Launch job sequentially")
@click.argument("id", type=str)
def resume_job(redis_url, testing, id):
    return _resume_job(redis_url, id, testing)


@click.command("show-job", help="List all jobs, can be filter")
@click.option("--id", type=str, help="Filter by job id")
@click.option("--status", type=str, help="Filter by job status")
@click.option("--limit", help="Limit to show", default=50)
def show_job(id, status, limit):
    from fsapp.command import get_job_by_query

    res = get_job_by_query(id=id, status=status, limit=limit)
    print(res)


@click.command("show-user", help="Show user bucket access")
@click.option("--user", type=str, help="Filter by user")
@click.option("--bucket", type=str, help="Filter by bucket")
@click.option("--limit", help="Limit to show", default=10)
def show_user_bucket(user, bucket, limit):
    from fsapp.command import get_user_permission

    res = get_user_permission(user=user, bucket=bucket, limit=limit)
    print(res)


@click.command("show-file", help="List all files, can be filter")
@click.option("--filename", type=str, help="Filter by file name")
@click.option(
    "--source_path", type=str, help="Filter by directory where file is transfered"
)
@click.option("--bucket", type=str, help="Filter by bucket where file is transfered")
@click.option(
    "--prefix", type=str, help="Filter by used prefix wher efile is transfered"
)
@click.option("--status", type=str, help="Filter by file status")
@click.option("--user", type=str, help="Filter by user")
@click.option("--job_id", type=str, help="Filter by Job_id")
@click.option("--limit", help="Limit to show", default=10)
def show_file(filename, source_path, bucket, prefix, status, user, job_id, limit):
    from fsapp.command import get_file_by_query

    res = get_file_by_query(
        filename, source_path, bucket, prefix, status, user, job_id, limit
    )
    print(res)


@click.command(
    "manage-user", help="Command to manager user permission, action is 'add' or 'rm'"
)
@click.argument("action", type=str)
@click.option("--user")
@click.option("--bucket")
@click.option("--allowed-root-dirs", required=True, type=click.Path(exists=True))
def manage_user(action, user, bucket, allowed_root_dirs):
    from fsapp.command import add_user, remove_user

    assert action in ['add', 'rm'], 'action must be add or rm'
    if action == "add":
        add_user(user, bucket, allowed_root_dirs)
    elif action == "rm":
        remove_user(user, bucket)
