import shlex

import pytest
from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


@pytest.fixture(scope="function")
def environ_m(monkeypatch):
    monkeypatch.setenv("OVERRIDE_PLATFORM", "linux")


# @pytest.mark.skip(reason="native")
# @pytest.mark.run
def test_linux_install(environ_m):
    """testing  general install 'real path'process for linux and unspecified drive letter"""
    command_string = "install-utils install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


pytest.mark.skip(reason="native")


# @pytest.mark.setup
def test_windows_install():
    """testing install into 'real path' not temporary test path process
      (--force option due to test environment being windows 11)
    and unspecified drive letter
    """
    command_string = "install-utils install --force-install --roaming='RoamingDev'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.setup
# @pytest.mark.skip(reason="native")
def test_create_schema():
    """testing  create schema with no existing schema"""
    command_string = "install-utils create-schema --roaming='RoamingDev'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.setup
# @pytest.mark.skip(reason="native")
def test_init_db():
    """testing  initializing database with no previous initialization"""
    command_string = "install-utils init-database --roaming='RoamingDev'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.skip(reason="purge")
def test_ipfs_purge():
    """testing  initializing database with previous initialization"""
    command_string = "ipfs-purge"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.skip(reason="purge")
def test_refresh_name():
    """testing  initializing database with previous initialization"""
    command_string = "refresh-name"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.skip(reason="danger")
# @pytest.mark.setup
def test_cli_l2_c_danger():
    """testing install into 'real path' not temporary test path process (--force option due to test environment being windows 11)
    and unspecified drive letter"""
    command_string = (
        "danger"  # NOTE: need to add --force-purge maybe with hidden option?
    )
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
