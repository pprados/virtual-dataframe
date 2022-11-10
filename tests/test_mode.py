from typing import Dict

import pandas
import pytest as pytest

import virtual_dataframe as vdf
from virtual_dataframe import VClient
from virtual_dataframe.env import USE_GPU
from .conftest import save_context, restore_context

_old_environ: Dict[str, str] = None


def setup_module(module):
    save_context()


def teardown_module(module):
    restore_context()


SimpleDF = vdf.VDataFrame


def _test_scenario_dataframe():
    @vdf.delayed
    def f_df(data: SimpleDF) -> SimpleDF:
        return data

    @vdf.delayed
    def f_series(data: SimpleDF) -> SimpleDF:
        return data

    input_df = vdf.VDataFrame({"data": [1, 2]}, npartitions=2)
    input_series = vdf.VSeries([1, 2], npartitions=2)

    # Try to_pandas()
    input_df.to_pandas()
    input_series.to_pandas()

    # Try compute()
    input_df.compute()
    input_series.compute()

    rc1 = f_df(input_df).compute()
    rc2 = vdf.compute(f_series(input_df))[0]
    assert rc1.equals(rc2)
    return input_df, rc1


@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.pandas, reason="Incompatible mode")
def test_DataFrame_MODE_pandas():
    import pandas
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, pandas.DataFrame)
        assert isinstance(rc, pandas.DataFrame)


@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.modin, reason="Incompatible mode")
def test_DataFrame_MODE_modin():
    import modin
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, modin.pandas.DataFrame)
        assert isinstance(rc, modin.pandas.DataFrame)


@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.dask_modin, reason="Incompatible mode")
def test_DataFrame_MODE_dask_modin():
    import modin
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, modin.pandas.DataFrame)
        assert isinstance(rc, modin.pandas.DataFrame)


# @pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.ray_modin, reason="Incompatible mode")
# def test_DataFrame_MODE_ray_modin():
#     import modin
#     import ray
#     with (VClient()):
#       input_df, rc = _test_scenario_dataframe()
#       assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
#       assert isinstance(input_df, modin.pandas.DataFrame)
#       assert isinstance(rc, modin.pandas.DataFrame)
#
#
@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.dask, reason="Incompatible mode")
def test_DataFrame_MODE_dask():
    import dask
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, dask.dataframe.DataFrame)
        assert isinstance(rc, pandas.DataFrame)


@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.cudf, reason="Incompatible mode")
def test_DataFrame_MODE_cudf():
    if not USE_GPU:
        pytest.skip("GPU Not found")
    import cudf
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, cudf.DataFrame)
        assert isinstance(rc, cudf.DataFrame)


@pytest.mark.skipif(vdf.VDF_MODE != vdf.Mode.dask_cudf, reason="Incompatible mode")
def test_DataFrame_MODE_dask_cudf():
    if not USE_GPU:
        pytest.skip("GPU Not found")
    import cudf
    import dask
    with (VClient()) as client:
        input_df, rc = _test_scenario_dataframe()
        assert SimpleDF({"data": [1, 2]}).to_pandas().equals(rc.to_pandas())
        assert isinstance(input_df, dask.dataframe.DataFrame)
        assert isinstance(rc, cudf.DataFrame)
