import typer
from rich import print
from typing_extensions import Annotated
from app import setup_db

app = typer.Typer()

@app.command()
def list(
    job_id: Annotated[str, typer.Option()] = None,
    source: Annotated[str, typer.Option()] = None,
    destination: Annotated[str, typer.Option()] = None,
    limit: Annotated[str, typer.Option()] = None,
    status: Annotated[str, typer.Option()] = None,
):
    from app.command import get_item_by_query

    session = setup_db()
    res = get_item_by_query(session, source, destination, status, job_id, limit)
    print(res)

if __name__ == "__main__":
    app()