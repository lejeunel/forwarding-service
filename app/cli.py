import click

from .worker import _init_and_upload, _resume
from rich import print
from .schemas import JobSchema


@click.command("upload", help="Upload data command")
@click.option("--regexp", default=".*", help="Regular expression")
@click.argument("source", type=str)
@click.argument("destination", type=str)
def upload(source, destination, regexp):
    job = _init_and_upload(source, destination, regexp)
    print(JobSchema().dump(job))


@click.command("resume", help="resume job")
@click.argument("id", type=str)
def resume(id):
    return _resume(id)


@click.command("list-job", help="List jobs")
@click.option("--id", type=str)
@click.option("--status", type=str)
@click.option("--limit", default=50)
def list_job(id, status, limit):
    from app.command import get_job_by_query

    res = get_job_by_query(id=id, status=status, limit=limit)
    print(res)


@click.command("list-item", help="List items")
@click.option("--source", "-s", type=str)
@click.option("--destination", "-d", type=str)
@click.option("--status", type=str)
@click.option("--job_id", type=str)
@click.option("--limit", default=10)
def list_item(source, destination, status, job_id, limit):
    from app.command import get_item_by_query

    res = get_item_by_query(source, destination, status, job_id, limit)
    print(res)
