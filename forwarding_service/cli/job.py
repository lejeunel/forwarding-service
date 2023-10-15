import typer
from forwarding_service.job_manager import JobManager
from forwarding_service.query import Query, JobQueryArgs
from forwarding_service import make_session
from forwarding_service.models import Job
from rich import print
from typing_extensions import Annotated

app = typer.Typer()


@app.command()
def run(
    source: Annotated[str, typer.Argument()],
    destination: Annotated[str, typer.Argument()],
    regexp: Annotated[str, typer.Option()] = ".*",
    n_threads: Annotated[int, typer.Option()] = 30,
):
    """Run job"""
    jm = JobManager.local_to_s3(n_threads=n_threads)
    job = jm.init(source, destination, regexp)
    print("created job", job.id)
    jm.parse_and_commit_items(job)
    print("parsed job", job.id)
    jm.run(job)
    print("finished job", job.id)


@app.command()
def resume(
    id: Annotated[str, typer.Argument()],
    n_threads: Annotated[int, typer.Option()] = 30,
):
    """Resume job"""
    jm = JobManager.local_to_s3(n_threads=n_threads)
    query = Query(make_session(), Job)
    if query.exists(id):
        jm.resume(query.get(JobQueryArgs(id=id))[0])
    else:
        f'{id} not found'


@app.command()
def ls(
    id: Annotated[str, typer.Option()] | None = None,
    status: Annotated[str, typer.Option()] | None = None,
    error: Annotated[str, typer.Option()] | None = None,
    source: Annotated[str, typer.Option()] | None = None,
    destination: Annotated[str, typer.Option()] | None = None,
    limit: Annotated[int, typer.Option()] = 50,
):
    """list jobs"""
    args = dict(locals())
    query = Query(make_session(), Job)
    jobs = query.get(JobQueryArgs(**args))
    jobs = [job.to_detailed_dict() for job in jobs]
    print(jobs)


@app.command()
def rm(
    id: Annotated[str, typer.Argument()],
):
    """Delete job and related items"""
    query = Query(make_session(), Job)
    query.delete(JobQueryArgs(id=id))


if __name__ == "__main__":
    app()
