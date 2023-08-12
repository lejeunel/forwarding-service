import logging

import click

from .worker import _upload, _resume_job
from rich import print

logging.basicConfig(level=logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


@click.command("upload", help="Upload data command")
@click.option("--regexp", default=".*", help="Regular expression")
@click.argument("source", type=str)
@click.argument("destination", type=str)
def upload(source, destination, regexp):
    return _upload(source, destination, regexp)


@click.command("resume-job")
@click.argument("id", type=str)
def resume_job(id):
    return _resume_job(id)


@click.command("show-job", help="List all jobs, can be filter")
@click.option("--id", type=str, help="Filter by job id")
@click.option("--status", type=str, help="Filter by job status")
@click.option("--limit", help="Limit to show", default=50)
def show_job(id, status, limit):
    from app.command import get_job_by_query

    res = get_job_by_query(id=id, status=status, limit=limit)
    print(res)


@click.command("show-file", help="List all files, can be filter")
@click.option("--filename", type=str, help="Filter by file name")
@click.option(
    "--source", "-s", type=str, help="Filter by directory where file is transfered"
)
@click.option("--destination", "-d", type=str, help="Filter by bucket where file is transfered")
@click.option("--status", type=str, help="Filter by file status")
@click.option("--job_id", type=str, help="Filter by Job_id")
@click.option("--limit", help="Limit to show", default=10)
def show_item(source, destination, status, job_id, limit):
    from app.command import get_item_by_query

    res = get_item_by_query(
        source, destination, status, job_id, limit
    )
    print(res)
