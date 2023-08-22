import typer
from rich import print
from typing_extensions import Annotated
from app.command import get_job_by_query
from app.models import Base
from app.transfer_agent import TransferAgent
from app.file import FileSystemReader
from app.s3 import S3Writer
from app.auth import S3StaticCredentials
from sqlalchemy.orm import Session
from decouple import config

app = typer.Typer()

def make_agent():
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///uploads.db")
    Base.metadata.create_all(engine)
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    auth = S3StaticCredentials(aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                               aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))
    writer = S3Writer(auth)
    agent = TransferAgent(session, reader=FileSystemReader(), writer=writer)

    return agent, session, connection, transaction


@app.command()
def upload(
    source: Annotated[str, typer.Argument()],
    destination: Annotated[str, typer.Argument()],
    regexp: Annotated[str, typer.Option()] = ".*",
    n_procs: Annotated[int, typer.Option()] = 1,
):
    agent, session, connection, transaction = make_agent()
    job = agent.init_job(source, destination, regexp)
    print('created job', job.to_detailed_dict())
    agent.parse_and_commit_items(job.id)
    print('parsed job', job.to_detailed_dict())
    job = agent.upload(job.id)
    print('finished job', job.to_detailed_dict())

    session.close()
    transaction.rollback()
    connection.close()

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
    res = get_job_by_query(id=id, status=status, limit=limit)
    print(res)

if __name__ == "__main__":
    app()
