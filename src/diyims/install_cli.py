from typing import Optional

import typer
from rich import print
from typing_extensions import Annotated
import os

from diyims.database_install import create, init
from diyims.error_classes import (
    ApplicationNotInstalledError,
    CreateSchemaError,
    InvalidDriveLetterError,
    PreExistingInstallationError,
    UnSupportedIPFSVersionError,
    UnSupportedPlatformError,
    UnTestedPlatformError,
)
from diyims.install import install_main

app = typer.Typer(no_args_is_help=True, help="Installation activities.")


@app.command()
def install(
    drive_letter: Annotated[
        Optional[str],
        typer.Option(
            help="The drive letter to use if not the default eg 'C:', note the colon.",
            show_default=False,
            rich_help_panel="Install Options",
        ),
    ] = "Default",
    force_install: Annotated[
        bool, typer.Option(help="Force installation", rich_help_panel="Install Options")
    ] = False,
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Install Options",
        ),
    ] = "Roaming",
):
    """
    The installation process is intended to satisfy the needs of most users. That being said,
        there may be circumstances which result in error messages. The most common error may well be
        the error for an untested platform referring to Windows 10 or 11. This is because Microsoft
        introduced the
        Microsoft Store which is one option to install Python. Unfortunately, the behavior
        of how the system handles directories is different than if Python was installed from the
        Python.org but there is no method to detect source of the Python installation. An option
        --force-python has been provided to force the installer to accept the Python installation
        no matter the source.

    """
    os.environ["ROAMING"] = str(roaming)
    try:
        install_main(drive_letter, force_install)

    except UnTestedPlatformError as error:
        print(
            error.system,
            "is an untested platform if Python was installed via the Microsoft Store application.",
        )

        raise typer.Exit(code=1)

    except PreExistingInstallationError:
        print("A previous installation was detected. Current installation not changed.")
        raise typer.Exit(code=1)

    except InvalidDriveLetterError as error:
        print(f"Provided drive letter {error.value} is invalid.")
        raise typer.Exit(code=2)

    except UnSupportedPlatformError as error:
        print(error.value, "is an unsupported platform.")
        raise typer.Exit(code=2)


@app.command()
def create_schema(
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Install Options",
        ),
    ] = "Roaming",
):
    """Initializes the database to a known state. If a pre-existing
    installation exists it will simply return with an error message
    """
    os.environ["ROAMING"] = str(roaming)
    try:
        create()
    except CreateSchemaError as error:
        print(
            f"There was a schema creation problem. If {error.value} is about a table already existing then this is simply a symptom of an existing installation. No changes were made."
        )
        raise typer.Exit(code=1)

    except ApplicationNotInstalledError:
        print("The application infrastructure not yet installed.")
        raise typer.Exit(code=1)


@app.command()
def init_database(
    roaming: Annotated[
        Optional[str],
        typer.Option(
            help="Set alternate Roaming value.",
            show_default=False,
            rich_help_panel="Install Options",
        ),
    ] = "Roaming",
):
    """Populates the Network_Peers table with a single entry to reflect this
    Network Node.
    If a pre-existing installation exists it will simply return with an error
    message

    """
    os.environ["ROAMING"] = str(roaming)
    try:
        init()

    except PreExistingInstallationError:
        print("Previous installation found. Current installation not changed.")
        raise typer.Exit(code=1)

    except UnSupportedIPFSVersionError as error:
        print(f"{error.value} is not supported")
        raise typer.Exit(code=2)
