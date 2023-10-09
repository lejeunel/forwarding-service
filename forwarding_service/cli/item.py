import typer
from forwarding_service.job_manager import JobManager
from forwarding_service.query import ItemQueryArgs
from rich import print
from typing_extensions import Annotated

app = typer.Typer()


@app.command()
def ls(
    id: Annotated[str, typer.Option()] = None,
    job_id: Annotated[str, typer.Option()] = None,
    source: Annotated[str, typer.Option()] = None,
    destination: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = 50,
    sort_on: Annotated[str, typer.Option()] = None,
):
    """list items"""

    jm = JobManager.inspector()
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
    items = [item.to_dict() for item in items]
    print(items)


if __name__ == "__main__":
    app()
