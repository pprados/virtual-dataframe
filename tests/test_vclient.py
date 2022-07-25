import importlib
import os

import pytest

import virtual_dataframe.env as env
import virtual_dataframe.vclient as vclient
import virtual_dataframe.vpandas as vpd
from tests.test_vpandas import SimpleDF


def _clean_env():
    if "DEBUG" in os.environ:
        os.environ.pop("DEBUG")
    if "USE_DASK" in os.environ:
        os.environ.pop("USE_DASK")
    if "USE_GPU" in os.environ:
        os.environ.pop("USE_GPU")

def _test_scenario_dataframe():
    @vpd.delayed
    def f_df(data: SimpleDF) -> SimpleDF:
        return data

    input_df = vpd.VDataFrame({"data": [1, 2]}, npartitions=2)

    rc1 = f_df(input_df).compute()
    return input_df, rc1

@pytest.mark.xdist_group(name="os.environ")
def test_dask_debug():
    os.environ["DEBUG"] = "Yes"
    os.environ["VDM_MODE"] = "dask"
    importlib.reload(vpd)
    _ = vclient.VClient()
    _, rc = _test_scenario_dataframe()
    assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))

@pytest.mark.xdist_group(name="os.environ")
def test_dask_cluster_gpu():
    os.environ["DEBUG"] = "False"
    os.environ["VDM_MODE"] = "dask-cudf"
    os.environ["DASK_SCHEDULER_SERVICE_HOST"] = "localhost"
    importlib.reload(vpd)
    _ = vclient.VClient()
    with (vclient.VClient()):
        _, rc = _test_scenario_dataframe()
        assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))

@pytest.mark.xdist_group(name="os.environ")
def test_dask_no_cluster_gpu():
    os.environ["DEBUG"] = "False"
    os.environ["VDM_MODE"] = "dask"
    if "DASK_SCHEDULER_SERVICE_HOST" in os.environ:
        os.environ.pop("DASK_SCHEDULER_SERVICE_HOST")
    importlib.reload(vpd)
    with (vclient.VClient()):
        _, rc = _test_scenario_dataframe()
        assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))

@pytest.mark.xdist_group(name="os.environ")
def test_dask_cluster_no_gpu():
    os.environ["DEBUG"] = "False"
    os.environ["VDM_MODE"] = "dask"
    os.environ["DASK_SCHEDULER_SERVICE_HOST"] = "localhost"
    importlib.reload(vpd)
    with (vclient.VClient()):
        _, rc = _test_scenario_dataframe()
        assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
