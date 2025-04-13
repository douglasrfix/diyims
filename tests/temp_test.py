import shlex


from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


def test_run_test():
    """testing  general install 'real path'process for linux and unspecified drive letter"""
    command_string = "run-test --roaming='ProdRoaming'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
