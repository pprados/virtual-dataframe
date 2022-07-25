import importlib
import os
import sys
import types

import pytest

import cudf
import dask
import pandas
import pytest as pytest

import virtual_dataframe.vpandas as vpd
import pandera.typing.pandas


class SimpleDF_schema(pandera.SchemaModel):
    id: pandera.typing.Index[int]
    data: pandera.typing.Series[int]

    class Config:
        strict = True
        ordered = True


SimpleDF = pandera.typing.DataFrame[SimpleDF_schema]

# FIXME
# def walk_reload(module: types.ModuleType) -> None:
#     print(f"walk_reload({module=}")
#     if hasattr(module, "__all__"):
#         for submodule_name in module.__all__:
#             walk_reload(getattr(module, submodule_name))
#         # importlib.reload(module)

def _test_scenario_dataframe():
    @vpd.delayed
    @pandera.check_types
    def f_df(data: SimpleDF) -> SimpleDF:
        return data

    @vpd.delayed
    @pandera.check_types
    def f_series(data: SimpleDF) -> SimpleDF:
        return data

    input_df = vpd.VDataFrame({"data": [1, 2]}, npartitions=2)
    input_series = vpd.VSeries([1, 2], npartitions=2)

    # Try to_pandas()
    input_df.to_pandas()
    input_series.to_pandas()

    # Try compute()
    input_df.compute()
    input_series.compute()

    rc1 = f_df(input_df).compute()
    rc2 = vpd.compute(f_series(input_df))[0]
    assert rc1.equals(rc2)
    return input_df, rc1

@pytest.mark.xdist_group(name="os.environ")
def test_DataFrame_MODE_pandas():
    os.environ["VDF_MODE"] = "pandas"
    del sys.modules["virtual_dataframe.env"]
    importlib.reload(vpd)

    input_df, rc = _test_scenario_dataframe()
    assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
    assert isinstance(input_df, pandas.DataFrame)
    assert isinstance(rc, pandas.DataFrame)

@pytest.mark.xdist_group(name="os.environ")
def test_DataFrame_MODE_dask():
    os.environ["VDF_MODE"] = "dask"
    del sys.modules["virtual_dataframe.env"]
    importlib.reload(vpd)

    input_df, rc = _test_scenario_dataframe()
    assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
    assert isinstance(input_df, dask.dataframe.DataFrame)
    assert isinstance(rc, pandas.DataFrame)

@pytest.mark.xdist_group(name="os.environ")
def test_DataFrame_MODE_cudf():
    os.environ["VDF_MODE"] = "cudf"
    del sys.modules["virtual_dataframe.env"]
    importlib.reload(vpd)

    input_df, rc = _test_scenario_dataframe()
    assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
    assert isinstance(input_df, cudf.DataFrame)
    assert isinstance(rc, cudf.DataFrame)

@pytest.mark.xdist_group(name="os.environ")
def test_DataFrame_MODE_dask_cudf():
    os.environ["VDF_MODE"] = "dask_cudf"
    del sys.modules["virtual_dataframe.env"]
    importlib.reload(vpd)

    input_df, rc = _test_scenario_dataframe()
    assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
    assert isinstance(input_df, dask.dataframe.DataFrame)
    assert isinstance(rc, cudf.DataFrame)
