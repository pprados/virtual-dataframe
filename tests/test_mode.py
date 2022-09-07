# import importlib
# import os
# import sys
# from typing import Dict
#
#
# import pandas
# import modin
# import pytest as pytest
#
# import virtual_dataframe.vpandas as vpd
# from virtual_dataframe.env import USE_GPU
# from .conftest import save_context, restore_context
#
#
# _old_environ: Dict[str, str] = None
#
#
# def setup_module(module):
#     save_context()
#
# def teardown_module(module):
#     restore_context()
#
#
# class SimpleDF_schema(pandera.SchemaModel):
#     id: pandera.typing.Index[int]
#     data: pandera.typing.Series[int]
#
#     class Config:
#         strict = True
#         ordered = True
#
#
# SimpleDF = pandera.typing.DataFrame[SimpleDF_schema]
#
#
# def _test_scenario_dataframe():
#     @vpd.delayed
#     @pandera.check_types
#     def f_df(data: SimpleDF) -> SimpleDF:
#         return data
#
#     @vpd.delayed
#     @pandera.check_types
#     def f_series(data: SimpleDF) -> SimpleDF:
#         return data
#
#     input_df = vpandas.VDataFrame({"data": [1, 2]}, npartitions=2)
#     input_series = vpandas.VSeries([1, 2], npartitions=2)
#
#     # Try to_pandas()
#     input_df.to_pandas()
#     input_series.to_pandas()
#
#     # Try compute()
#     input_df.compute()
#     input_series.compute()
#
#     rc1 = f_df(input_df).compute()
#     rc2 = vpandas.compute(f_series(input_df))[0]
#     assert rc1.equals(rc2)
#     return input_df, rc1
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_pandas():
#     os.environ["VDF_MODE"] = "pandas"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, pandas.DataFrame)
#     assert isinstance(rc, pandas.DataFrame)
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_modin():
#     os.environ["VDF_MODE"] = "modin"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, modin.pandas.DataFrame)
#     assert isinstance(rc, modin.pandas.DataFrame)
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_modin():
#     os.environ["VDF_MODE"] = "dask_modin"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, modin.pandas.DataFrame)
#     assert isinstance(rc, modin.pandas.DataFrame)
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_dask():
#     os.environ["VDF_MODE"] = "dask"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#     import dask
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, dask.dataframe.DataFrame)
#     assert isinstance(rc, pandas.DataFrame)
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_cudf():
#     if not USE_GPU:
#         pytest.skip("GPU Not found")
#     import cudf
#     os.environ["VDF_MODE"] = "cudf"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, cudf.DataFrame)
#     assert isinstance(rc, cudf.DataFrame)
#
#
# @pytest.mark.xdist_group(name="os.environ")
# @pytest.mark.skip(reason="Border effet")
# def test_DataFrame_MODE_dask_cudf():
#     if not USE_GPU:
#         pytest.skip("GPU Not found")
#     import cudf
#     import dask
#     os.environ["VDF_MODE"] = "dask_cudf"
#     del sys.modules["virtual_dataframe.env"]
#     importlib.reload(vpandas)
#
#     input_df, rc = _test_scenario_dataframe()
#     assert rc.to_pandas().equals(SimpleDF({"data": [1, 2]}))
#     assert isinstance(input_df, dask.dataframe.DataFrame)
#     assert isinstance(rc, cudf.DataFrame)
