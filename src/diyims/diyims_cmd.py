"""This is the command line interface driver.

It provides CLI access to each of the applications functions

It is part of an installable package so does not
need the if __name__ == "__main__":.

"""

import os
import typer
from typing import Optional
from typing_extensions import Annotated

from diyims import install_cli
from diyims import beacon_cli
from diyims.scheduler import scheduler_main
from diyims.general_utils import clean_up, shutdown_cmd
from diyims.ipfs_utils import purge, refresh_network_name, force_purge
from diyims.queue_server import queue_main
from diyims.peer_capture import capture_peer_main
from diyims.capture_want_lists import capture_peer_want_lists
from diyims.test import test


app = typer.Typer(
    no_args_is_help=True, help="Base command for the DIY Independent Media Services."
)
# app.add_typer(database_cli.app, name="database")
# app.add_typer(configuration_cli.app, name="config")
app.add_typer(install_cli.app, name="install-utils")
app.add_typer(beacon_cli.app, name="beacon-utils")


@app.command()
def danger():
    force_purge()


@app.command()
def shutdown():
    shutdown_cmd()


@app.command()
def refresh_name():
    refresh_network_name()


@app.command()
def ipfs_purge():
    """
    ipfs purge for test cid.

    """
    purge()


@app.command()
def capture_providers():
    capture_peer_main("PP")


@app.command()
def capture_swarm_peers():
    capture_peer_main("SP")


@app.command()
def capture_bitswap_peers():
    capture_peer_main("BP")


@app.command()
def run_scheduler(
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Execution Options",
        ),
    ] = "Roaming",
):
    os.environ["ROAMING"] = str(roaming)

    scheduler_main()


@app.command()
def run_clean_up(
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Execution Options",
        ),
    ] = "Roaming",
):
    os.environ["ROAMING"] = str(roaming)

    clean_up()


@app.command()
def run_queue_server():
    queue_main()


@app.command()
def capture_want_lists(
    peer_type: Annotated[
        Optional[str], typer.Option(help="Peer Type", rich_help_panel="Peer Type")
    ] = "PP",
):
    capture_peer_want_lists(peer_type)


@app.command()
def run_test(
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Execution Options",
        ),
    ] = "Roaming",
):
    os.environ["ROAMING"] = str(roaming)

    test()
