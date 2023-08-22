import typer
from rich import print
from typing_extensions import Annotated

from app.command import get_job_by_query
from app import make_agent, setup_db

app = typer.Typer()


@app.command()
def upload(
    source: Annotated[str, typer.Argument()],
    destination: Annotated[str, typer.Argument()],
    regexp: Annotated[str, typer.Option()] = ".*",
    n_procs: Annotated[int, typer.Option()] = 1,
):
    agent, session = make_agent()
    job = agent.init_job(source, destination, regexp)
    print('created job', job.to_detailed_dict())
    agent.parse_and_commit_items(job.id)
    print('parsed job', job.to_detailed_dict())
    job = agent.upload(job.id)
    print('finished job', job.to_detailed_dict())

    session.close()

@app.command()
def resume(id: Annotated[str, typer.Argument()],
           n_procs: Annotated[int, typer.Option()] = 1,
           ):
    pass


@app.command()
def list(
    id: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = None,
):
    session = setup_db()
    res = get_job_by_query(session, id=id, status=status, limit=limit)
    print(res)

if __name__ == "__main__":
    app()
