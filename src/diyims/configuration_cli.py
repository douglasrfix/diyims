import typer
from rich import print

# from config_utils import list_config

app = typer.Typer(no_args_is_help=True, help="Configuration activities.")


@app.command()
def help():
    """This is to cath some of the looking form help attempts"""
    print("Try --help or diyims ")

    # @app.command()
    # def list():
    """Initializes the database to a known state. If a pre-existing
    installation exists it will simply return with an error message
    """


#    list_config()
