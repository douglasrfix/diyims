import pytest


@pytest.fixture(autouse=True)
def dep_warning(monkeypatch):
    monkeypatch.setenv("PYTHONDEVMODE", "1")
