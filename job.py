import typer
from rich import print
from typing_extensions import Annotated
from app.command import get_job_by_query

app = typer.Typer()

@app.command()
def upload(
    source: Annotated[str, typer.Argument()],
    destination: Annotated[str, typer.Argument()],
    regexp: Annotated[str, typer.Option()] = ".*",
):
    pass


@app.command()
def resume(id: Annotated[str, typer.Argument()]):
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
