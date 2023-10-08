import typer
from rich import print
from typing_extensions import Annotated

from forwarding_service.job_manager import JobManager
from forwarding_service.query import ItemQueryArgs

app = typer.Typer()


@app.command()
def ls(
    id: Annotated[str, typer.Option()] = None,
    job_id: Annotated[str, typer.Option()] = None,
    source: Annotated[str, typer.Option()] = None,
    destination: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = 50,
    status: Annotated[str, typer.Option()] = None,
    sort_on: Annotated[str, typer.Option()] = None,
):
    """list items"""

    jm = JobManager.local_to_s3()
    items = jm.query.items(
        ItemQueryArgs(
            id=id,
            source=source,
            destination=destination,
            status=status,
            job_id=job_id,
            limit=limit,
            sort_on=sort_on,
        )
    )
    items = [dict(item) for item in items]
    print(items)


if __name__ == "__main__":
    app()
