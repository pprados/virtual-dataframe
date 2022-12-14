# Technical point of view

`virtual_dataframe` framework patch others frameworks to unify the API.

`VDF_MODE`:

- Pandas like
    - [pandas](#pandas)
    - [cudf](#cudf)
    - [modin](#modin-or-dask_modin)
    - [dask](#dask)
    - [dask_cudf](#dask_cudf)
    - [pyspark](#pyspark)
- Numpy like
    - [numpy](#numpy)
    - [cupy](#cupy)
    - [dask_array](#dask_array)

# Pandas like frameworks

## Pandas

- Add `vdf.BackEndDataFrame = pandas.DataFrame`
- Add `vdf.BackEndSeries = pandas.Series`
- Add `vdf.BackEndArray = numpy.ndarray`
- Add `vdf.BackEndPandas = pandas`
- Add `vdf.FrontEndPandas = pandas`
- Add `vdf.FrontEndNumpy = numpy`

- Add `vdf.compute()` to return a tuple of args and be compatible with [`dask.compute()`](https://docs.dask.org/en/stable/api.html#dask.compute)
- Add `vdf.concat()` an alias of `panda.concat()`
- Add `vdf.delayed()` to delay a calland be compatible with [`dask.delayed()`](https://docs.dask.org/en/stable/delayed.html)
- Add `vdf.persist()` to parameters and empty image and be compatible with [`dask.persist()`](https://docs.dask.org/en/stable/persist.html)
- Add `vdf.visualize()` to return an empty image and be compatible with [`dask.visualize()`](https://docs.dask.org/en/stable/api.html#dask.visualize)

- Add `vdf.from_pandas()` to return df and be compatible with [`dask.from_pandas()](https://docs.dask.org/en/stable/generated/dask.dataframe.from_pandas.html)
- Add `vdf.from_backend()` an alias of `from_pandas()`

- Add `vdf.numpy` an alias of `numpy` module

- Remove extra parameters used by Dask in:
  - `*.to_csv()`, `*.to_excel()`, `*.to_feather()`, `*.to_hdf()`, `*.to_json()`
- Update the pandas API to accept glob filename in:
  - `vdf.read_csv()`, `vdf.read_excel()`, `vdf.read_feather()`, `vdf.read_fwf()`, `vdf.read_hdf`, `vdf.read_json()`,
`vdf.read_orc()`, `vdf.read_parquet()`, `vdf.read_sql_table()`
  - `DF.to_csv()`, `DF.to_excel()`, `DF.to_feather()`, `DF.to_hdf()`, `DF.to_json()`,
  - `Series.to_csv()`, `Series.to_excel()`, `Series.to_hdf()`, `Series.to_json()`
- Add methods with `_not_implemented`
  - `DF.to_fwf()`
- Add `DF.to_pandas()` to return `self`
- Add `DF.to_backend()` to return `self`
- Add `DF.to_ndarray()` an alias to `to_numpy()`
- Add `DF.apply_rows()` to be compatible with [`cudf.apply_rows()`](https://docs.rapids.ai/api/cudf/nightly/api_docs/api/cudf.DataFrame.apply_rows.html)
- Add `DF.map_partitions()` to be compatible with [`dask.map_partitions()`](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.map_partitions.html)
- Add `DF.compute()` to return `self` and be compatible with [`dask.DataFrame.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.compute.html)
- Add `DF.repartition()` to return `self` and be compatible with [`dask.DataFrame.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html#dask.dataframe.DataFrame.repartition)
- Add `DF.visualize()` to return `visualize(self)` and be compatible with [`dask.DataFrame.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.visualize.html)
- Add `DF.categorize()` to return `self` and be compatible with [`dask.DataFrame.categorize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.categorize.html)

- Add `Series.to_pandas()` to return `self`
- Add `Series.to_backend()` to return `self`
- Add `Series.to_ndarray()` alias of `to_numpy`

- Add `Series.compute()` to return `self` and be compatible with [`dask.Series.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.compute.html)
- Add `Series.map_partitions()` to return `self.map()` and be compatible with [`dask.Series.map_partitions()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.map_partitions.html)
- Add `Series.persist()` to return `self` and be compatible with ['dask.Series.persist()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.persist.html)
- Add `Series.repartition()` to return `self` and be compatible with ['dask.Series.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.repartition.html)
- Add `Series.visualize()` to return `visualize(self)` and be compatible with ['dask.Series.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.visualize.html)

## cudf
- Add `vdf.BackEndDataFrame = cudf.DataFrame`
- Add `vdf.BackEndSeries = cudf.Series`
- Add `vdf.BackEndArray = cupy.ndarray`
- Add `vdf.BackEndPandas = cudf`
- Add `vdf.FrontEndPandas = cudf`
- Add `vdf.FrontEndNumpy = cupy`

- Add `vdf.compute()` to return an tuple of args and be compatible with [`dask.compute()`](https://docs.dask.org/en/stable/api.html#dask.compute)
- Add `vdf.concat()` an alias of `panda.concat()`
- Add `vdf.delayed()` to delay a calland be compatible with [`dask.delayed()`](https://docs.dask.org/en/stable/delayed.html)
- Add `vdf.persist()` to parameters and empty image and be compatible with [`dask.persist()`](https://docs.dask.org/en/stable/persist.html)
- Add `vdf.visualize()` to return an empty image and be compatible with [`dask.visualize()`](https://docs.dask.org/en/stable/api.html#dask.visualize)

- Add `vdf.from_pandas()` to return df and be compatible with [`dask.from_pandas()](https://docs.dask.org/en/stable/generated/dask.dataframe.from_pandas.html)
- Add `vdf.from_backend()` an alias of `from_pandas()`

- Add `vdf.numpy` an alias of `cupy` module

- Remove extra parameters used by Dask in:
  - `*.to_csv()`, `*.to_excel()`, `*.to_feather()`, `*.to_hdf()`, `*.to_json()`
- Update the pandas API to accept glob filename in:
  - `vdf.read_csv()`, `vdf.read_feather()`, `vdf.read_json()`
  - `DF.to_csv()`, `DF.to_excel()`, `DF.to_feather()`, `DF.to_hdf()`, `DF.to_json()`,
  - `Series.to_hdf()`, `Series.to_json()`
- Add methods with `_not_implemented`
  - `vdf.read_excel()`, `vdf.read_fwf()`, `vdf.read_sql_table()`
  - `DF.to_csv()`, `DF.to_excel()`
- Add `pandas.DataFrame.to_pandas()` to return `self`
- Add `DF.to_backend()` to return `self`
- Add `DF.to_ndarray()` to convert DataFrame to `cupy.ndarray`
- Add `DF.map_partitions()` to be compatible with [`dask.map_partitions()`](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.map_partitions.html)
- Add `DF.compute()` to return `self` and be compatible with [`dask.DataFrame.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.compute.html)
- Add `DF.repartition()` to return `self` and be compatible with [`dask.DataFrame.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html#dask.dataframe.DataFrame.repartition)
- Add `DF.visualize()` to return `visualize(self)` and be compatible with [`dask.DataFrame.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.visualize.html)
- Add `DF.categorize()` to return `self` and be compatible with [`dask.DataFrame.categorize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.categorize.html)

- Add `pandas.Series.to_pandas()` to return `self`
- Add `Series.to_backend()` to return `self`
- Add `Series.to_ndarray()` alias of `to_numpy`

- Add `Series.compute()` to return `self` and be compatible with [`dask.Series.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.compute.html)
- Add `Series.map_partitions()` to return `self.map()` and be compatible with [`dask.Series.map_partitions()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.map_partitions.html)
- Add `Series.persist()` to return `self` and be compatible with ['dask.Series.persist()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.persist.html)
- Add `Series.repartition()` to return `self` and be compatible with ['dask.Series.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.repartition.html)
- Add `Series.visualize()` to return `visualize(self)` and be compatible with ['dask.Series.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.visualize.html)

## modin or dask_modin
- Set `MODIN_ENGINE=dask` for `dask_modin`
- Set `MODIN_ENGINE=python` for `modin`
- Add `vdf.BackEndDataFrame = modin.pandas.DataFrame`
- Add `vdf.BackEndSeries = modin.pandas.Series`
- Add `vdf.BackEndArray = numpy.ndarray`
- Add `vdf.BackEndPandas = modin.pandas`
- Add `vdf.FrontEndPandas = modin.pandas`
- Add `vdf.FrontEndNumpy = numpy`

- Add `vdf.compute()` to return a tuple of args and be compatible with [`dask.compute()`](https://docs.dask.org/en/stable/api.html#dask.compute)
- Add `vdf.concat()` an alias of `modin.pandas.concat()`
- Add `vdf.delayed()` to delay a calland be compatible with [`dask.delayed()`](https://docs.dask.org/en/stable/delayed.html)
- Add `vdf.persist()` to parameters and empty image and be compatible with [`dask.persist()`](https://docs.dask.org/en/stable/persist.html)
- Add `vdf.visualize()` to return an empty image and be compatible with [`dask.visualize()`](https://docs.dask.org/en/stable/api.html#dask.visualize)

- Add `vdf.from_pandas()` to return modin DataFrame or Series and be compatible with [`dask.from_pandas()](https://docs.dask.org/en/stable/generated/dask.dataframe.from_pandas.html)
- Add `vdf.from_backend()` an alias of `from_pandas()`

- Add `vdf.numpy` an alias of `numpy` module

- Remove extra parameters used by Dask in:
  - `*.to_csv()`, `*.to_excel()`, `*.to_feather()`, `*.to_hdf()`, `*.to_json()`
- Add warning when using:
  - `read_excel()`, `read_feather()`, `read_fwf()`, `read_hdf()`, `read_sql_table()`
  - `DF.to_excel()`, `DF.to_feather()`, `DF.to_hdf()`, `DF.to_sql()`
  - `Series.to_csv()`, `Series.to_excel()`, `Series.to_hdf()`, `Series.to_json()`
- Update the pandas API to accept glob filename in:
  - `vdf.read_excel()`, `vdf.read_feather()`, `vdf.read_fwf()`, `vdf.read_hdf`,
`vdf.read_orc()`
  - `DF.to_excel()`, `DF.to_feather()`, `DF.to_hdf()`, `DF.to_sql()`
  - `Series.to_csv()`, `Series.to_excel()`, `Series.to_hdf()`, `Series.to_json()`
- Add methods with `_not_implemented`
  - `DF.to_orc()`
- Add `DF.to_pandas()` to convert to `panda.DataFrame`
- Add `DF.to_backend()` to return `self`
- Add `DF.to_ndarray()` an alias to `to_numpy()`

- Add `DF.apply_rows()` to be compatible with [`cudf.apply_rows()`](https://docs.rapids.ai/api/cudf/nightly/api_docs/api/cudf.DataFrame.apply_rows.html)
- Add `DF.map_partitions()` to be compatible with [`dask.map_partitions()`](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.map_partitions.html)
- Add `DF.compute()` to return `self` and be compatible with [`dask.DataFrame.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.compute.html)
- Add `DF.repartition()` to return `self` and be compatible with [`dask.DataFrame.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html#dask.dataframe.DataFrame.repartition)
- Add `DF.visualize()` to return `visualize(self)` and be compatible with [`dask.DataFrame.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.visualize.html)
- Add `DF.categorize()` to return `self` and be compatible with [`dask.DataFrame.categorize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.categorize.html)

- Add `Series.to_pandas()` to return `modin.pandas.Series.to_pandas()`
- Add `Series.to_backend()` to return `self`
- Add `Series.to_ndarray()` alias of `to_numpy`

- Add `Series.compute()` to return `self` and be compatible with [`dask.Series.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.compute.html)
- Add `Series.map_partitions()` to return `self.map()` and be compatible with [`dask.Series.map_partitions()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.map_partitions.html)
- Add `Series.persist()` to return `self` and be compatible with ['dask.Series.persist()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.persist.html)
- Add `Series.repartition()` to return `self` and be compatible with ['dask.Series.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.repartition.html)
- Add `Series.visualize()` to return `visualize(self)` and be compatible with ['dask.Series.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.visualize.html)

- And all patch in [pandas](#pandas)


## dask
- Add `vdf.BackEndDataFrame = pandas.DataFrame`
- Add `vdf.BackEndSeries = pandas.Series`
- Add `vdf.BackEndArray = numpy.ndarray`
- Add `vdf.BackEndPandas = pandas`
- Add `vdf.FrontEndPandas = dask.dataframe`
- Add `vdf.FrontEndNumpy = dask.array`

- Add `vdf.concat()` an alias of `dask.dataframe.multi.concat()`

- Add `vdf.from_pandas()` an alias of `dask.dataframe.from_pandas()`
- Add `vdf.from_backend()` an alias of `from_pandas()`

- Add `vdf.numpy` an alias of `numpy` module

- Add warning in:
  - `read_fwf()`, `read_hdf()`, `read_sql_table()`
- Add methods with `_not_implemented`
  - `read_excel()`, `read_feather()`
  - `DF.to_excel()`, `DF.to_feather()`, `DF.to_fwf()`
- Add `DF.to_pandas()` to return `self.compute()`
- Add `DF.to_backend()` an alias of `to_pandas()`
- Add `DF.to_numpy()` to return `self.compute().to_numpy()`
- Add `DF.to_ndarray()` an alias to `dask.DataFrame.to_dask_array()`
- Add `DF.apply_rows()` to be compatible with [`cudf.apply_rows()`](https://docs.rapids.ai/api/cudf/nightly/api_docs/api/cudf.DataFrame.apply_rows.html)
- Patch `DF.to_sql()` and `Series.to_sql()` to accept `con` or `uri`
- Add `Series.to_pandas()` to return `self.compute()`
- Add `Series.to_backend()` an alias of `to_pandas()`
- Add `Series.to_numpy()` to return `self.compute().to_numpy()`
- Add `Series.to_ndarray()` alias of [`dask.dataframe.Series.to_dask_array()`](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.to_dask_array.html)

- And all patch in [pandas](#pandas)

## dask_cudf
- Add `vdf.BackEndDataFrame = cudf.DataFrame`
- Add `vdf.BackEndSeries = cudf.Series`
- Add `vdf.BackEndArray = cudf`
- Add `vdf.BackEndPandas = pandas`
- Add `vdf.FrontEndPandas = dask_cudf`
- Add `vdf.FrontEndNumpy = cupy`

- Add `vdf.compute()` to `dask.compute()`
- Add `vdf.concat()` to `dask.dataframe.multi.concat()`
- Add `vdf.delayed()` to `dask.delayed()`
- Add `vdf.persist()` to `dask.persist()`
- Add `vdf.visualize()` to `dask.visualize()`

- Add `vdf.from_pandas()` to `dask_cudf.from_cudf()`
- Add `vdf.from_backend()` to `dask_cudf.from_cudf()`

- Add `vdf.numpy` an alias of `cupy` module

- Add a warning in:
  - `Series.to_hdf()`, `Series.to_json()`
- Add methods with `_not_implemented`
  - `read_excel()`, `read_feather()`, `read_fwf()`, `read_hdf()`, `read_sql_table()`
  - `DF.to_excel()`, `DF.to_feather()`, `DF.to_fwf()`, `DF.to_hdf()`,`DF.to_sql()`,
  - `Series.to_csv()`, `Series.to_excel()`,
- Add `DF.to_pandas()` to return `self.compute().to_pandas()`
- Add `DF.to_backend()` to return `self.compute()` and return `cudf.DataFrame`
- Add `DF.to_numpy()` to `self.compute().to_numpy()`
- Add `DF.to_ndarray()` an alias to `self.compute()` and return `cudf.DataFrame`

- Add `Series.to_pandas()` to return `self.compute().to_pandas()`
- Add `Series.to_backend()` to return `self.compute()` and return `cudf.Series`
- Add `Series.to_numpy()` to return `self.compute().to_numpy()`
- Add `Series.to_ndarray()` to return a `cudf.Series`

- Add `Series.compute()` to return `self` and be compatible with [`dask.Series.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.compute.html)
- Add `Series.map_partitions()` to return `self.map()` and be compatible with [`dask.Series.map_partitions()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.map_partitions.html)
- Add `Series.persist()` to return `self` and be compatible with ['dask.Series.persist()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.persist.html)
- Add `Series.repartition()` to return `self` and be compatible with ['dask.Series.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.repartition.html)
- Add `Series.visualize()` to return `visualize(self)` and be compatible with ['dask.Series.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.visualize.html)

- And all patch in [cudf](#cudf)

## pyspark
- Add `vdf.BackEndDataFrame = pandas.DataFrame`
- Add `vdf.BackEndSeries = pandas.Series`
- Add `vdf.BackEndArray = numpy.ndarray`
- Add `vdf.BackEndPandas = pandas`
- Add `vdf.FrontEndPandas = pyspark.pandas`
- Add `vdf.FrontEndNumpy = numpy`

- Add `vdf.compute()` to return a tuple of args and be compatible with [`dask.compute()`](https://docs.dask.org/en/stable/api.html#dask.compute)
- Add `vdf.concat()` an alias of `pyspark.pandas.concat()`
- Add `vdf.delayed()` to delay a call and be compatible with [`dask.delayed()`](https://docs.dask.org/en/stable/delayed.html)
- Add `vdf.persist()` to persist the current DF
- Add `vdf.visualize()` to return an empty image and be compatible with [`dask.visualize()`](https://docs.dask.org/en/stable/api.html#dask.visualize)

- Add `vdf.from_backend()` an alias of `from_pandas()`

- Add `vdf.numpy` an alias of `numpy` module

- Remove extra parameters used by Dask in:
  - `*.to_csv()`, `*.to_excel()`, `*.to_feather()`, `*.to_hdf()`, `*.to_json()`
  - `from_pandas()`
- Add warning in:
  - `read_excel()`, `reql_sql_table()`
- Update the pandas API to accept glob filename in:
  - `vdf.read_csv()`, `vdf.read_excel()`, `vdf.read_json()`, `vdf.read_orc()`
  - `DF.to_csv()`, `DF.to_excel()`, `DF.to_feather()`, `DF.to_hdf()`, `DF.to_json()`,
  - `Series.to_csv()`, `Series.to_excel()`, `Series.to_hdf()`, `Series.to_json()`
- Add methods with `_not_implemented`
  - `vdf.read_feather()`, `vdf.read_fwf()`, `vdf.read_hdf()`
  - `DF.to_sql()`, `Series.to_sql()`
- Add `DF.to_backend()` an alias of `to_pandas()`
- Add `DF.to_ndarray()` an alias to `to_numpy()`

- Add `DF.apply_rows()` to be compatible with [`cudf.apply_rows()`](https://docs.rapids.ai/api/cudf/nightly/api_docs/api/cudf.DataFrame.apply_rows.html)
- Add `DF.categorize()` to return `self` and be compatible with [`dask.DataFrame.categorize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.categorize.html)
- Add `DF.compute()` to return `self` and be compatible with [`dask.DataFrame.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.compute.html)
- Add `DF.map_partitions()` to be compatible with [`dask.map_partitions()`](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.map_partitions.html)
- Add `DF.persist()` to return `self` and be compatible with [`dask.DataFrame.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.visualize.html)
- Add `DF.repartition()` to return `self` and be compatible with [`dask.DataFrame.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html#dask.dataframe.DataFrame.repartition)
- Add `DF.visualize()` to return `visualize(self)` and be compatible with [`dask.DataFrame.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.visualize.html)

- Add `Series.to_backend()` alias of `to_pandas()`
- Add `Series.to_ndarray()` alias of `to_numpy()`

- Add `Series.compute()` to return `self` and be compatible with [`dask.Series.compute()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.compute.html)
- Add `Series.map_partitions()` to return `self.map()` and be compatible with [`dask.Series.map_partitions()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.map_partitions.html)
- Add `Series.persist()` to return `self` and be compatible with ['dask.Series.persist()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.persist.html)
- Add `Series.repartition()` to return `self` and be compatible with ['dask.Series.repartition()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.repartition.html)
- Add `Series.visualize()` to return `visualize(self)` and be compatible with ['dask.Series.visualize()](https://docs.dask.org/en/stable/generated/dask.dataframe.Series.visualize.html)

# Numpy like familly

## Numpy

It's not possible to update some method in `numpy.ndarray`.

- `vdf.numpy` is an alias of `numpy`
- Add `vdf.numpy.asnumpy(ar)` to return `ar`
- Add `vdf.numpy.asndarray(ar)` to return `ar.to_numpy()`
- Add `vdf.numpy.compute(...)` to return a tuple with parameters
- Add `vdf.numpy.compute_chunk_sizes(ar)` to return `ar`
- Add `vdf.numpy.rechunk(ar)` to return `ar`
- Add `vdf.numpy.arange()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.from_array()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.load()` to remove the parameter `chunks`
- Add `vdf.numpy.save()` to remove the parameter `chunks`
- Add `vdf.numpy.savez()` to remove the parameter `chunks`
- Add `vdf.numpy.random.*` to remove the parameter `chunks`

## cupy

- `vdf.numpy` is an alias of `cupy`
- Add `vdf.numpy.asndarray(ar)` to return `ar.to_numpy()`
- Add `vdf.numpy.compute(...)` to return a tuple with parameters
- Add `vdf.numpy.compute_chunk_sizes(ar)` to return `ar`
- Add `vdf.numpy.rechunk(ar)` to return `ar`
- Add `vdf.numpy.arange()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.from_array()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.load()` to remove the parameter `chunks`
- Add `vdf.numpy.save()` to remove the parameter `chunks`
- Add `vdf.numpy.savez()` to remove the parameter `chunks`
- Add `vdf.numpy.random.*` to remove the parameter `chunks`

## dask_array

- `vdf.numpy` is an alias of `dasl.array`
- Add `vdf.numpy.asarray(ar)` to return array of numpy or cupy
- Add `vdf.numpy.asndarray(ar)` to return `ar.to_numpy()`
- Add `vdf.numpy.compute(...)` to return a tuple with parameters
- Add `vdf.numpy.compute_chunk_sizes(ar)` to return `ar`
- Add `vdf.numpy.rechunk(ar)` to return `ar`
- Add `vdf.numpy.arange()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.from_array()`, remove the parameter `chunks`, invoke `numpy.arange()` and return a view with `Vndarray`
- Add `vdf.numpy.load()` to remove the parameter `chunks`
- Add `vdf.numpy.save()` to remove the parameter `chunks`
- Add `vdf.numpy.savez()` to remove the parameter `chunks`
- Add `vdf.numpy.random.*` to remove the parameter `chunks`
