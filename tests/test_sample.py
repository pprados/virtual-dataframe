import pytest

import virtual_dataframe as vpd
from virtual_dataframe import *

TestDF = VDataFrame


@delayed
def sample_function(data: TestDF) -> TestDF:
    return data


def test_sample():
    with (VClient()) as client:
        vdf = vpd.VDataFrame({"data": [1, 2]})
        rc = sample_function(vdf).compute()
        assert (vdf.to_backend()
                == rc.to_backend()).all().all()
