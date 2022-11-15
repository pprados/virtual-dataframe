## API

| api                                    | comments                                          |
|----------------------------------------|---------------------------------------------------|
| vdf.@delayed                           | Delayed function (do nothing or dask.delayed)     |
| vdf.concat(...)                        | Merge VDataFrame                                  |
| vdf.read_csv(...)                      | Read VDataFrame from CSVs *glob* files            |
| vdf.read_excel(...)<sup>*</sup>        | Read VDataFrame from Excel *glob* files           |
| vdf.read_fwf(...)<sup>*</sup>          | Read VDataFrame from Fwf *glob* files             |
| vdf.read_hdf(...)<sup>*</sup>          | Read VDataFrame from HDFs *glob* files            |
| vdf.read_json(...)                     | Read VDataFrame from Jsons *glob* files           |
| vdf.read_orc(...)                      | Read VDataFrame from ORCs *glob* files            |
| vdf.read_parquet(...)                  | Read VDataFrame from Parquets *glob* files        |
| vdf.read_sql_table(...)<sup>*</sup>    | Read VDataFrame from SQL                          |
| vdf.from_pandas(pdf, npartitions=...)  | Create Virtual Dataframe from Pandas DataFrame    |
| vdf.from_backend(vdf, npartitions=...) | Create Virtual Dataframe from backend dataframe   |
| vdf.compute([...])                     | Compute multiple @delayed functions               |
| VDataFrame(data, npartitions=...)      | Create DataFrame in memory (only for test)        |
| VSeries(data, npartitions=...)         | Create Series in memory (only for test)           |
| VLocalCluster(...)                     | Create a dask Local Cluster (Dask, Cuda or Spark) |
| BackEndDataFrame                       | The class of dask backend dataframe               |
| BackEndSeries                          | The class of dask backend series                  |
| BackEnd                                | The backend framework                             |
| VDataFrame.compute()                   | Compute the virtual dataframe                     |
| VDataFrame.persist()                   | Persist the dataframe in memory                   |
| VDataFrame.repartition()               | Rebalance the dataframe                           |
| VDataFrame.visualize()                 | Create an image with the graph                    |
| VDataFrame.to_pandas()                 | Convert to pandas dataframe                       |
| VDataFrame.to_csv()                    | Save to *glob* files                              |
| VDataFrame.to_excel()<sup>*</sup>      | Save to *glob* files                              |
| VDataFrame.to_feather()<sup>*</sup>    | Save to *glob* files                              |
| VDataFrame.to_hdf()<sup>*</sup>        | Save to *glob* files                              |
| VDataFrame.to_json()                   | Save to *glob* files                              |
| VDataFrame.to_orc()                    | Save to *glob* files                              |
| VDataFrame.to_parquet()                | Save to *glob* files                              |
| VDataFrame.to_sql()<sup>*</sup>        | Save to sql table                                 |
| VDataFrame.to_numpy()                  | Convert to numpy array                            |
| VDataFrame.to_backend()                | Convert to backend dataframe                      |
| VDataFrame.categorize()                | Detect all categories                             |
| VDataFrame.apply_rows()                | Apply rows, GPU template                          |
| VDataFrame.map_partitions()            | Apply function for each parttions                 |
| VSeries.compute()                      | Compute the virtual series                        |
| VSeries.persist()                      | Persist the dataframe in memory                   |
| VSeries.repartition()                  | Rebalance the dataframe                           |
| VSeries.visualize()                    | Create an image with the graph                    |
| VSeries.to_pandas()                    | Convert to pandas series                          |
| VSeries.to_backend()                   | Convert to backend series                         |
| VSeries.to_numpy()                     | Convert to numpy array                            |
| VClient(...)                           | The connexion with the cluster                    |
| VDF_MODE                               | The current mode                                  |
| Mode                                   | The enumeration of differents mode                |

<sup>*</sup> some frameworks do not implement it


You can read a sample notebook [here](https://github.com/pprados/virtual-dataframe/blob/master/notebooks/demo.ipynb)
for an exemple of all API.

Each API propose a specific version for each framework. For example:

- the  `toPandas()` with Panda, return `self`
- `@delayed` use the dask `@delayed` or do nothing, and apply the code when the function was called.
In the first case, the function return a part of the dask graph. In the second case, the function return immediately
the result.
- `read_csv("file*.csv")` read each file in parallele with dask or spark by each worker,
or read sequencially each file and combine each dataframe with a framework without distribution (pandas, modin, cudf)
- `save_csv("file*.csv")` write each file in parallele with dask or spark,
or write one file `"file.csv"` (without the star) with a framework without distribution (pandas, modin, cudf)
- ...

