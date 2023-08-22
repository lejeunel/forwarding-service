import typer
import job, item

app = typer.Typer()
app.add_typer(job.app, name='jobs')
app.add_typer(item.app, name='items')

if __name__ == "__main__":
    app()
