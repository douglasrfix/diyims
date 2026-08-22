import os
import shlex

import pytest
from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


@pytest.fixture(scope="session")
def environ(tmp_path_factory):
    p = str(tmp_path_factory.mktemp("session"))
    os.environ["OVERRIDE_HOME"] = p


@pytest.fixture(scope="function")
def environ_v(monkeypatch):
    monkeypatch.setenv("OVERRIDE_IPFS_VERSION", "0")


# NOTE: Need linux installation


def test_cli_l2_c1_b(environ):
    """testing  install with --force option windows 11
    this should also be okay for windows 10"""
    command_string = "install-utils install --force-install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


def test_cli_l2_c1_c(environ):
    """testing  install with --force option windows 11
    this should also be okay for windows 10 with a prior installation"""
    command_string = "install-utils install --force-install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


def test_cli_l2_c1_d(environ):
    """testing  create schema with no existing schema"""
    command_string = "install-utils create-schema"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


def test_cli_l2_c1_e(environ):
    """testing  create schema with existing schema"""
    command_string = "install-utils create-schema"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


def test_cli_l2_c1_f(environ_v, environ):
    """testing  initializing database with no previous initialization and unsupported ipfs version"""
    command_string = "install-utils init-database"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 2


def test_cli_l2_c1_f2(environ):
    """testing  initializing database with no previous initialization"""
    command_string = "install-utils init-database"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


def test_cli_l2_c1_g(environ):
    """testing  initializing database with previous initialization"""
    command_string = "install-utils init-database"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


# @pytest.mark.skip(reason="purge")   # NOTE: If this test is skipped you must force system purge to clean up
def test_cli_l2_c1_purge(environ):
    """testing  initializing database with previous initialization"""
    command_string = "ipfs-purge"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
