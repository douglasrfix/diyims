import shlex

import pytest
from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


@pytest.mark.component
def test_beacon():
    """testing find_providers with native windows install"""
    command_string = "capture-providers"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.component
def test_capture_providers():
    """testing find_providers with native windows install"""
    command_string = "capture-providers"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.component
def test_capture_want_lists():
    """testing find_providers with native windows install"""
    command_string = "capture-want-lists --peer-type 'BP'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.component
def test_capture_bitswap_peers():
    """testing find_providers with native windows install"""
    command_string = "capture-bitswap-peers"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


@pytest.mark.component
def test_capture_swarm_peers():
    """testing find_providers with native windows install"""
    command_string = "capture-swarm-peers"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.component
def test_clean_up():
    """testing clean_up"""
    command_string = "run-clean-up --roaming='RoamingDev'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
