import pytest


@pytest.fixture(autouse=True)
def dep_warning(monkeypatch):
    monkeypatch.setenv("PYTHONDEVMODE", "1")
    return


@pytest.fixture(autouse=True)
def set_component_test_true(monkeypatch):
    monkeypatch.setenv("DIYIMS_COMPONENT_TEST", "0")
    return


@pytest.fixture(autouse=True)
def set_queues_enabled_true(monkeypatch):
    monkeypatch.setenv("DIYIMS_QUEUES_ENABLED", "1")
    return


@pytest.fixture(autouse=True)
def set_roaming_to_dev(monkeypatch):
    monkeypatch.setenv("DIYIMS_ROAMING", "RoamingDev")
    return
