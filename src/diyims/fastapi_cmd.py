import os
import subprocess
from typing import Optional

import typer
from typing_extensions import Annotated

# from diyims.general_utils import exec_fastapi


app = typer.Typer(
    no_args_is_help=True, help="Base command for the DIY Independent Media Services."
)


@app.command()
def run_fastapi(
    roaming: Annotated[
        Optional[str],
        typer.Option(help="Set alternate Roaming value."),
    ] = "Roaming",
) -> None:
    os.environ["DIYIMS_ROAMING"] = str(roaming)
    subprocess.run(["fastapi", "dev", "fastapi_app.py"])

    return
