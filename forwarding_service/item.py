import typer
from rich import print
from typing_extensions import Annotated

from forwarding_service import make_session
from forwarding_service.command import get_item_by_query

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

    session = make_session()
    res = get_item_by_query(session, source, destination, status, job_id, limit, sort_on)
    print(res)

if __name__ == "__main__":
    app()
