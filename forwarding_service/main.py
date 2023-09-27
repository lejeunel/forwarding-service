import typer
from . import job, item

app = typer.Typer()
app.add_typer(job.app, name='job')
app.add_typer(item.app, name='item')

def main():
    app()

if __name__ == '__main__':
    main()
