import shlex

import pytest
from typer.testing import CliRunner

from diyims.diyims_cmd import app

runner = CliRunner()


@pytest.fixture(scope="function")
def environ_e(tmp_path, monkeypatch):
    p = str(tmp_path)
    monkeypatch.setenv("OVERRIDE_HOME", p)


@pytest.fixture(params=["unknown", "freebsd", "aix", "wasi", "cygwin", "darwin"])
def set_platform(request):
    return request.param


@pytest.fixture(scope="function")
def environ_u(monkeypatch, set_platform):
    monkeypatch.setenv("OVERRIDE_PLATFORM", set_platform)


@pytest.fixture(scope="function")
def environ_l(monkeypatch, tmp_path):
    monkeypatch.setenv("OVERRIDE_PLATFORM", "linux")
    p = str(tmp_path)
    monkeypatch.setenv("OVERRIDE_HOME", p)


@pytest.fixture(scope="function")
def environ_m(monkeypatch):
    monkeypatch.setenv("OVERRIDE_PLATFORM", "linux")


@pytest.fixture(scope="function")
def environ_h(tmp_path, monkeypatch):
    p = str(tmp_path)
    monkeypatch.setenv("OVERRIDE_HOME", p)


@pytest.fixture(scope="function")
def environ_p(monkeypatch, tmp_path):
    monkeypatch.setenv("OVERRIDE_RELEASE", "8")
    p = str(tmp_path)
    print(p)
    monkeypatch.setenv("OVERRIDE_HOME", p)


@pytest.fixture(scope="function")
def environ_q(monkeypatch, tmp_path):
    monkeypatch.setenv("OVERRIDE_RELEASE", "8")
    p = str(tmp_path)
    monkeypatch.setenv("OVERRIDE_HOME", p)
    monkeypatch.setenv("OVERRIDE_DRIVE", "True")


# @pytest.mark.skip(reason="menus")
def test_cli_l1_c0():
    """runner can't test the l1 command with noargs =true"""
    command_string = "diyims"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 2


# @pytest.mark.skip(reason="menus")
def test_cli_l1_c1():
    """runner can't test the l1 commands with the
    diyims command that would form the command line"""
    command_string = "diyims install-utils"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 2


# @pytest.mark.skip(reason="menus")
def test_cli_l2_c0():
    """This tests the help when no args passed without diyims"""
    command_string = "install-utils"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.skip(reason="not installed")
# @pytest.mark.run
def test_cli_l2_c1_a0(environ_u):
    """testing  install with unsupported platform"""
    command_string = "install-utils install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 2


def test_cli_l2_c1_a01(environ_e):
    """testing  db create before install (--force required for test platform)"""
    command_string = "install-utils create-schema"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


def test_cli_l2_c1_a02(environ_e):
    """testing  db init before install (--force required for test platform)"""
    command_string = "install-utils init-database"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


# @pytest.mark.skip(reason="install")
def test_cli_l2_c1_a1(environ_e):
    """testing  install with no option windows 11
    this should also test for windows 10 should generate untested platform error"""
    command_string = "install-utils install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 1


# @pytest.mark.skip(reason="menus")
def test_cli_l2_c1_a2(environ_e):
    """testing  install with drive option windows 11
    this should also be okay for windows 10"""
    command_string = "install-utils install --drive-letter 'C'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 2


# @pytest.mark.skip(reason="menus")
def test_cli_l2_c1_a3(environ_p):
    """testing  install with drive option windows 11
    this should also be okay for windows 10"""
    command_string = "install-utils install --drive-letter 'C:'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.skip(reason="menus")
def test_cli_l2_c1_a4(environ_q):
    """testing  install with drive option windows 11
    this should also be okay for windows 10"""
    command_string = "install-utils install --drive-letter 'D:'"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.skip(reason="menus")
# @pytest.mark.run
def test_cli_l2_c1_b00(environ_h):
    """testing  general install process (--force option due to test environment being windows 11)
    and unspecified drive letter"""
    command_string = "install-utils install --force-install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0


# @pytest.mark.skip(reason="menus")
# @pytest.mark.run
def test_cli_l2_c1_b01(environ_l):
    """testing  general install process for linux and unspecified drive letter"""
    command_string = "install-utils install"
    result = runner.invoke(app, shlex.split(command_string))
    print(result.stdout.rstrip())
    assert result.exit_code == 0
