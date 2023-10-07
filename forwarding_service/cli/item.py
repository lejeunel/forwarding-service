import typer
from rich import print
from typing_extensions import Annotated

from forwarding_service.job_manager import JobManager

app = typer.Typer()

@app.command()
def ls(
    job_id: Annotated[str, typer.Option()] = None,
    source: Annotated[str, typer.Option()] = None,
    destination: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
    sort_on: Annotated[str, typer.Option()] = None,
):
    """list items"""

    jm = JobManager.local_to_s3()
    items = jm.query.items(ItemQueryArgs(source, destination, status, job_id, limit, sort_on))
    items = [item.to_dict() for item in items]
    print(items)

if __name__ == "__main__":
    app()
