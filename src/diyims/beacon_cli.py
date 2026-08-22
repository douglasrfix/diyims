import typer
from diyims.beacon import beacon_main, satisfy_main

app = typer.Typer(
    no_args_is_help=True, help="Execution of the Beacon function and subsets."
)


@app.command()
def beacon_execution():
    """
    Populates the Network_Peers table with a single entry to reflect this
    Network Node.
    If a pre-existing installation exists it will simply return with an error
    message
    """

    beacon_main("beacon_execution")
    return


@app.command()
def satisfy_execution():
    """
    Populates the Network_Peers table with a single entry to reflect this
    Network Node.
    If a pre-existing installation exists it will simply return with an error
    message
    """

    satisfy_main("satisfy_execution")
    return
