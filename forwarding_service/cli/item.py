import typer
from forwarding_service import make_session
from forwarding_service.models import Item
from forwarding_service.query import ItemQueryArgs, Query
from rich import print
from typing_extensions import Annotated

app = typer.Typer()


@app.command()
def ls(
    id: Annotated[str, typer.Option()] | None = None,
    job_id: Annotated[str, typer.Option()] | None = None,
    source: Annotated[str, typer.Option()] | None = None,
    destination: Annotated[str, typer.Option()] | None = None,
    status: Annotated[str, typer.Option()] | None = None,
    limit: Annotated[int, typer.Option()] = 50,
    sort_on: Annotated[str, typer.Option()] | None = None,
):
    """list items"""

    args = dict(locals())
    query = Query(make_session(), Item)
    items = query.get(
        ItemQueryArgs(
            **args
    ))
    items = [dict(item) for item in items]
    print(items)


if __name__ == "__main__":
    app()
