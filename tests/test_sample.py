import pytest

import virtual_dataframe as vpd
from virtual_dataframe import *
from virtual_dataframe.sample import sample_function


@pytest.fixture(scope="session")
def vclient():
    return VClient()

def test_sample(vclient):
    with (vclient):
        vdf = vpd.VDataFrame({"data": [1, 2]})
        rc = sample_function(vdf).compute()
        assert rc.equals(vpd.VDataFrame({"data": [1, 2]}).compute())

