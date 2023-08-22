import typer
from rich import print
from typing_extensions import Annotated

app = typer.Typer()

@app.command()
def list_item(
    job_id: Annotated[str, typer.Option()] = None,
    source: Annotated[str, typer.Option()] = None,
    destination: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
):
    from app.command import get_item_by_query

    res = get_item_by_query(source, destination, status, job_id, limit)
    print(res)

if __name__ == "__main__":
    app()
