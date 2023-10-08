import typer
from forwarding_service.job_manager import JobManager
from forwarding_service.query import JobQueryArgs
from forwarding_service.enum_types import JobError, ItemStatus
from rich import print
from typing_extensions import Annotated

app = typer.Typer()


@app.command()
def run(
    source: Annotated[str, typer.Argument()],
    destination: Annotated[str, typer.Argument()],
    regexp: Annotated[str, typer.Option()] = ".*",
    n_procs: Annotated[int, typer.Option()] = 1,
):
    """Run job"""
    jm = JobManager.local_to_s3(n_procs=n_procs)
    job = jm.init(source, destination, regexp)
    if job.error != JobError.INIT_ERROR:
        print("created job", job.id)
        jm.parse_and_commit_items(job.id)
        print("parsed job", job.id)
        jm.run(job.id)
        print("finished job", job.id)
    else:
        print(f"Error: {str(job.error)} {job.info}")

@app.command()
def resume(
    id: Annotated[str, typer.Argument()],
    n_procs: Annotated[int, typer.Option()] = 1,
):
    """Resume job"""
    jm = JobManager.local_to_s3(n_procs=n_procs)
    jm.resume(id)


@app.command()
def ls(
    id: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
    error: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = 50,
):
    """list jobs"""
    jm = JobManager.local_to_s3()
    jobs = jm.query.jobs(JobQueryArgs(id=id, status=status, limit=limit, error=error))
    jobs = [{**dict(job), **{'total_files': len(job.items),
                             'done_files': len([item for item in job.items if item.status == ItemStatus.TRANSFERRED])}} for job in jobs]
    print(jobs)

@app.command()
def rm(
    id: Annotated[str, typer.Argument()],
):
    """Delete job and related items"""
    jm = JobManager.local_to_s3()
    jm.delete_job(id)

if __name__ == "__main__":
    app()
