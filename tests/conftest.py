import pytest


@pytest.fixture(autouse=True)
def dep_warning(monkeypatch):
    monkeypatch.setenv("PYTHONDEVMODE", "1")
    return


def set_component_test_true(monkeypatch):
    monkeypatch.setenv("COMPONENT_TEST", "0")
    return
