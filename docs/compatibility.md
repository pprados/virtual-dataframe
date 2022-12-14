## Compatibility
This project is just a wrapper. So, it inherits limitations and bugs from other projects. Sorry for that.


| Limitations of Pandas like framework                                                             |
|--------------------------------------------------------------------------------------------------|
| <br />**pandas**                                                                                 |
| All data must be in DRAM                                                                         |
| <br />**modin**                                                                                  |
| [Read this](https://modin.readthedocs.io/en/stable/getting_started/why_modin/pandas.html)        |
| <br />**[cudf](https://docs.rapids.ai/api/cudf/nightly/user_guide/pandas-comparison.html)**      |
| All data must be in VRAM                                                                         |
| All data types in cuDF are nullable                                                              |
| Iterating over a cuDF Series, DataFrame or Index is not supported.                               |
| Join (or merge) and groupby operations in cuDF do not guarantee output ordering.                 |
| The order of operations is not always deterministic                                              |
| Cudf does not support duplicate column names                                                     |
| Cudf also supports .apply() it relies on Numba to JIT compile the UDF and execute it on the GPU  |
| .apply(result_type=...) not supported                                                            |
| <br />**[dask](https://distributed.dask.org/en/stable/limitations.html)**                        |
|  transpose() and MultiIndex are not implemented                                                  |
| Column assignment doesn't support type list                                                      |
| <br />**dask_cudf**                                                                              |
| See cudf and dask.                                                                               |
| Categories with strings not implemented                                                          |
| <br />**pyspark**                                                                                |
| [Read this](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) |


| Limitations of Numpy like framework                                     |
|-------------------------------------------------------------------------|
| <br/>**numpy**</br/>                                                    |
| All data must be in RAM                                                 |
| <br/>**cupy**</br/>                                                     |
| [Read this](https://docs.cupy.dev/en/stable/user_guide/difference.html) |
| - block() not implemented                                               |
| - delete() not implemented                                              |
| - insert() not implemented                                              |
| <br/>**dask array**</br/>                                               |
| [Read this](https://docs.dask.org/en/stable/array.html#scope)           |
| - identity() not implemented                                            |
| - asfarray() not implemented                                            |
| - asfortranarray() not implemented                                      |
| - ascontiguousarray() not implemented                                   |
| - asarray_chkfinite() not implemented                                   |
| - require() not implemented                                             |
| - column_stack() not implemented                                        |
| - row_stack() not implemented                                           |
| - \*split\*() not implemented                                           |
| - resize() not implemented                                              |
| - trim_zeros() not implemented                                          |
| - in1d() not implemented                                                |
| - intersect1d() not implemented                                         |
| - setdiff1d() not implemented                                           |
| - setxor1d() not implemented                                            |
| - column_stack() not implemented                                        |
| - row_stack() not implemented                                           |
| - fromiter() not implemented                                           |


For compatibility between numpy and cupy,
see [here](https://numpy.org/doc/stable/user/basics.interoperability.html#basics-interoperability).

### File format compatibility
To be compatible with all framework, you must only use the common features.
We accept some function to read or write files, but we write a warning
if you use a function not compatible with others frameworks.

| read_... / to_...         | pandas | cudf | modin | dask | dask_modin | dask_cudf | pyspark |
|---------------------------|:------:|:----:|:-----:|:----:|:----------:|:---------:|:-------:|
| **vdf.read_csv**          |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VDataFrame.to_csv**     |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VSeries.to_csv**        |   ✓    |      |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| vdf.read_excel            |   ✓    |      |   ✓   |      |            |           |    ✓    |
| VDataFrame.to_excel       |   ✓    |      |   ✓   |      |            |           |    ✓    |
| VSeries.to_excel          |   ✓    |      |   ✓   |      |            |           |    ✓    |
| vdf.read_feather          |   ✓    |  ✓   |   ✓   |      |            |           |         |
| VDataFrame.to_feather     |   ✓    |  ✓   |   ✓   |      |            |           |         |
| vdf.read_fwf              |   ✓    |      |   ✓   |  ✓   |     ✓      |           |         |
| vdf.read_hdf              |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |           |         |
| VDataFrame.to_hdf         |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |           |         |
| VSeries.to_hdf            |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |           |         |
| **vdf.read_json**         |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VDataFrame.to_json**    |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VSeries.to_json**       |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **vdf.read_orc**          |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VDataFrame.to_orc**     |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **vdf.read_parquet**      |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| **VDataFrame.to_parquet** |   ✓    |  ✓   |   ✓   |  ✓   |     ✓      |     ✓     |    ✓    |
| vdf.read_sql_table        |   ✓    |      |   ✓   |  ✓   |     ✓      |           |    ✓    |
| VDataFrame.to_sql         |   ✓    |      |   ✓   |  ✓   |     ✓      |           |    ✓    |
| VSeries.to_sql            |   ✓    |      |   ✓   |  ✓   |     ✓      |           |    ✓    |


| load... / save...  | numpy | cupy | dask.array |
|--------------------|:-----:|:----:|:----------:|
| **vpd.load() npy** |   ✓   |  ✓   |     ✓      |
| **vpd.save() npy** |   ✓   |  ✓   |     ✓      |
| vpd.savez() npz    |   ✓   |  ✓   |            |
| vpd.loadtxt()      |   ✓   |  ✓   |            |
| vpd.savetxt()      |   ✓   |  ✓   |            |


### Cross framework compatibility

|       | small data                   | middle data                | big data                                                        |
|-------|------------------------------|----------------------------|-----------------------------------------------------------------|
| 1-CPU | pandas, numpy<br/>*Limits:+* |                            |                                                                 |
| n-CPU |                              | modin, numpy<br/>*Limits+* | dask, dask_modin or pyspark and dask.array<br/>*Limits:++*      |
| GPU   | cudf, cupy<br/>*Limits:++*   |                            | dask_cudf, pyspark+spark-rapids and dask.array<br/>*Limits:+++* |

To develop, you can choose the level to be compatible with others frameworks.
Each cell is strongly compatible with the upper left part.

### No need of GPU?
If you don't need to use a GPU, then develop for `dask` and use mode in *bold*.

|       | small data                       | middle data                    | big data                                                        |
|-------|----------------------------------|--------------------------------|-----------------------------------------------------------------|
| 1-CPU | **pandas, numpy<br/>*Limits:+*** |                                |                                                                 |
| n-CPU |                                  | **modin, numpy<br/>*Limits+*** | **dask, dask_modin or pyspark and dask.array<br/>*Limits:++***  |
| *GPU* | *cudf, cupy<br/>Limits:++*       |                                | *dask_cudf, pyspark+spark-rapids and dask.array<br/>Limits:+++* |

You can ignore this API:

- `VDataFrame.apply_rows()`

### No need of big data?

If you don't need to use big data, then develop for `cudf` and use mode in *bold*..

|       | small data                       | middle data                    | big data                                                        |
|-------|----------------------------------|--------------------------------|-----------------------------------------------------------------|
| 1-CPU | **pandas, numpy<br/>*Limits:+*** |                                |                                                                 |
| n-CPU |                                  | **modin, numpy<br/>*Limits+*** | *dask, dask_modin or pyspark and dask.array<br/>Limits:++*      |
| GPU   | **cudf, cupy<br/>*Limits:++***   |                                | *dask_cudf, pyspark+spark-rapids and dask.array<br/>Limits:+++* |

You can ignore these API:

- `@delayed`
- `map_partitions()`
- `categorize()`
- `compute()`
- `npartitions=...`

### Need all possibility?

To be compatible with all modes, develop for `dask_cudf`.

|       | small data                   | middle data                | big data                                                        |
|-------|------------------------------|----------------------------|-----------------------------------------------------------------|
| 1-CPU | pandas, numpy<br/>*Limits:+* |                            |                                                                 |
| n-CPU |                              | modin, numpy<br/>*Limits+* | dask, dask_modin or pyspark and dask.array<br/>*Limits:++*      |
| GPU   | cudf, cupy<br/>*Limits:++*   |                            | dask_cudf, pyspark+spark-rapids and dask.array<br/>*Limits:+++* |

and accept all the limitations.
