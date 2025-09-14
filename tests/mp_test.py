from diyims.beacon import beacon_main, satisfy_main
from diyims.provider_capture import (
    provider_capture_main,
)
import pytest


# @pytest.mark.skip(reason="native")
# @pytest.mark.run
@pytest.mark.mp
# @pytest.mark.xdist_group(name="group1")
def test_beacon():
    beacon_main()


@pytest.mark.mp
# @pytest.mark.xdist_group(name="group2")
def test_satisfy():
    satisfy_main()


@pytest.mark.mp
# @pytest.mark.xdist_group(name="group2")
def test_capture_providers():
    provider_capture_main("PP")


@pytest.mark.mp
# @pytest.mark.xdist_group(name="group2")
def test_capture_bitswap():
    provider_capture_main("BP")


@pytest.mark.mp
# @pytest.mark.xdist_group(name="group2")
def test_capture_swarm():
    provider_capture_main("SP")
