import shlex

# import pytest
from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


# @pytest.mark.skip(reason="p")
def test_scheduler():
    """testing  initializing database with previous initialization"""
    command_string = "run-scheduler --roaming='ProdRoaming'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
