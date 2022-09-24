# Virtual DataFrame

[Full documentation](https://pprados.github.io/virtual_dataframe/)

## Motivation

With Panda like dataframe, do you want to create a code, and choose at the end, the framework to use?
Do you want to be able to choose the best framework after simply performing performance measurements?
This framework unifies multiple Panda-compatible components, to allow the writing of a single code, compatible with all.

## Synopsis

With some parameters and Virtual classes, it's possible to write a code, and execute this code:

- With or without multicore
- With or without cluster (multi nodes)
- With or without GPU

To do that, we create some virtual classes, add some methods in others classes, etc.

It's difficult to use a combinaison of framework, with the same classe name, with similare semantic, etc.
For example, if you want to use in the same program, Dask, cudf, pandas and modin, you must manage:

- `pandas.DataFrame`, `pandas,Series`
- `modin.pandas.DataFrame`, `modin.pandas.Series`
- `cudf.DataFrame`, `cudf.Series`
- `dask.DataFrame`, `dask.Series`

 With `cudf`, the code must call `.to_pandas()`. With dask, the code must call `.compute()`, can use `@delayed` or
`dask.distributed.Client`. etc.

We propose to replace all these classes and scenarios, with a *uniform model*,
inspired by [dask](https://www.dask.org/) (the more complex API).
Then, it is possible to write one code, and use it in differents environnements and frameworks.

This project is essentially a back-port of *Dask+Cudf* to others frameworks.
We try to normalize the API of all frameworks.

To reduce the confusion, you must use the classes `VDataFrame` and `VSeries` (The prefix `V` is for *Virtual*).
These classes propose the methods `.to_pandas()` and `.compute()` for each version, but are the *real* classes
of the selected framework.

A new `@delayed` annotation can be use, with or without Dask.

With some parameters, the real classes may be `pandas.DataFrame`, `modin.pandas.DataFrame`,
`cudf.DataFrame`, `dask.dataframe.DataFrame` with Pandas or
`dask.dataframe.DataFrame` with cudf (with Pandas or cudf for each partition).

To manage the initialisation of a Dask, you must use the `VClient()`. This alias, can be automatically
initialized with some environment variables.

```python
# Sample of code, compatible Pandas, cudf, modin, dask and dask_cudf
from virtual_dataframe import *

TestDF = VDataFrame

with (VClient()):
    @delayed
    def my_function(data: TestDF) -> TestDF:
        return data


    rc = my_function(VDataFrame({"data": [1, 2]}, npartitions=2))
    print(rc.to_pandas())

```

With this framework, you can select your environment, to run or debug your code.

| env                                                   | Environement                                |
|-------------------------------------------------------|---------------------------------------------|
| VDF_MODE=pandas                                       | Only Python with classical pandas           |
| VDF_MODE=modin                                        | Python with local modin                     |
| VDF_MODE=cudf                                         | Python with local cuDF (GPU)                |
| VDF_MODE=dask                                         | Dask with local multiple process and pandas |
| VDF_MODE=dask-cudf                                    | Dask with local multiple process and cuDF   |
| VDF_MODE=dask<br />DEBUG=True                         | Dask with single thread and pandas          |
| VDF_MODE=dask_cudf<br />DEBUG=True                    | Dask with single thread and cuDF            |
| VDF_MODE=dask<br />VDF_CLUSTER=dask://localhost       | Dask with local cluster and pandas          |
| VDF_MODE=dask_cudf<br />VDF_CLUSTER=dask://localhost  | Dask with local cuda cluster and cuDF       |
| VDF_MODE=dask<br />VDF_CLUSTER=dask://...:ppp         | Dask with remote cluster and Pandas         |
| VDF_MODE=dask_cudf<br />VDF_CLUSTER=dask://...:ppp    | Dask with remote cluster and cuDF           |
| VDF_MODE=dask_modin<br />VDF_CLUSTER=dask://localhost | Dask with local cluster and modin           |
| VDF_MODE=dask_modin<br />VDF_CLUSTER=dask://...:ppp   | Dask with remote cluster and modin          |

The real compatibilty between the differents simulation of Pandas, depends on the implement of the modin, cudf or dask.
Sometime, you can use the `VDF_MODE` variable, to update some part of code, between the selected backend.

It's not always easy to write a code *compatible* with all scenario, but it's possible.
Generally, add just `.compute()` and/or `.to_pandas()` at the end of the ETL, is enough.
But, you must use, only the common feature with all frameworks.
After this effort, it's possible to compare the performance about the differents technologies,
or propose a component, compatible with differents scenario.

For the deployment of your project, you can select the best framework for your process (in a dockerfile?),
with only one ou two environment variables.

## Cluster
To connect to a cluster, use `VDF_CLUSTER` with protocol, host and optionaly, the port.

- dask://locahost:8787
- ray://locahost:10001
- ray:auto
- or alternativelly, use `DASK_SCHEDULER_SERVICE_HOST` and `DASK_SCHEDULER_SERVICE_PORT`

| VDF_MODE   | DEBUG | VDF_CLUSTER      | Scheduler        |
|------------|-------|------------------|------------------|
| pandas     | -     | -                | No scheduler     |
| cudf       | -     | -                | No scheduler     |
| modin      | -     | -                | No scheduler     |
| dask       | Yes   | -                | synchronous      |
| dask       | No    | -                | thread           |
| dask       | No    | threads          | thread           |
| dask       | No    | processes        | processes        |
| dask       | No    | dask://localhost | LocalCluster     |
| dask_modin | No    | -                | LocalCluster     |
| dask_modin | No    | dask://localhost | LocalCluster     |
| dask_modin | No    | dask://<host>    | Dask cluster     |
| ray_modin  | No    | ray:auto         | Dask cluster     |
| ray_modin  | No    | ray://localhost  | Dask cluster     |
| ray_modin  | No    | ray://<host>     | Dask cluster     |
| dask-cudf  | No    | dask://localhost | LocalCUDACluster |
| dask-cudf  | No    | dask://<host>    | Dask cluster     |


Sample:
```
from virtual_dataframe import VClient

with (VClient())
    # Now, use the scheduler
```

## Installation
### Installing with Conda (recommended)
```shell
$ conda install -q -y \
	-c rapidsai -c nvidia -c conda-forge \
	"virtual_dataframe-all"
```
or, for only one mode:
```shell
$ VDF_MODE=...
$ conda install -q -y \
	-c rapidsai -c nvidia -c conda-forge \
	virtual_dataframe-$VDF_MODE
```
The package `virtual_dataframe` (without suffix) has no *framework* dependencies.

### Installing with pip
With PIP, it's not possible to install [NVidia Rapids](https://developer.nvidia.com/rapids) to use
the GPU. A limited list of dependencies is possible.

> :warning: **Warning: At this time, the packages are not published in pip or conda repositories**
Use
```shell
$ pip install "virtual_dataframe[all]@git+https://github.com/pprados/virtual-dataframe"
```
When the project were published, use
```shell
$ pip install "virtual_dataframe[all]"
```
or, for a selected framework
```shell
$ VDF_MODE=... # pandas, modin, dask or dask_modin only
$ pip install "virtual_dataframe[$VDF_MODE]"
```
The core of the framework can be installed with
```shell
$ pip install "virtual_dataframe"
```

### Installing from the GitHub master branch
```shell
$ pip install "virtual_dataframe[all]@git+https://github.com/pprados/virtual-dataframe"
```
or, for a selected framework
```shell
$ VDF_MODE=... # pandas, modin, dask or dask_modin only
$ pip install "virtual_dataframe[$VDF_MODE]@git+https://github.com/pprados/virtual-dataframe"
```
The core of the framework can be installed with
```shell
$ pip install "virtual_dataframe@git+https://github.com/pprados/virtual-dataframe"
```

## API

| api                                    | comments                                        |
|----------------------------------------|-------------------------------------------------|
| vdf.@delayed                           | Delayed function (do nothing or dask.delayed)   |
| vdf.concat(...)                        | Merge VDataFrame                                |
| vdf.read_csv(...)                      | Read VDataFrame from CSVs *glob* files          |
| vdf.read_excel(...)<sup>*</sup>        | Read VDataFrame from Excel *glob* files         |
| vdf.read_fwf(...)<sup>*</sup>          | Read VDataFrame from Fwf *glob* files           |
| vdf.read_hdf(...)<sup>*</sup>          | Read VDataFrame from HDFs *glob* files          |
| vdf.read_json(...)                     | Read VDataFrame from Jsons *glob* files         |
| vdf.read_orc(...)                      | Read VDataFrame from ORCs *glob* files          |
| vdf.read_parquet(...)                  | Read VDataFrame from Parquets *glob* files      |
| vdf.read_sql_table(...)<sup>*</sup>    | Read VDataFrame from SQL                        |
| vdf.from_pandas(pdf, npartitions=...)  | Create Virtual Dataframe from Pandas DataFrame  |
| vdf.from_backend(vdf, npartitions=...) | Create Virtual Dataframe from backend dataframe |
| vdf.compute([...])                     | Compute multiple @delayed functions             |
| VDataFrame(data, npartitions=...)      | Create DataFrame in memory (only for test)      |
| VSeries(data, npartitions=...)         | Create Series in memory (only for test)         |
| BackEndDataFrame                       | The class of dask/ray backend dataframe         |
| BackEndSeries                          | The class of dask/ray backend series            |
| BackEnd                                | The backend framework                           |
| VDataFrame.compute()                   | Compute the virtual dataframe                   |
| VDataFrame.persist()                   | Persist the dataframe in memory                 |
| VDataFrame.repartition()               | Rebalance the dataframe                         |
| VDataFrame.visualize()                 | Create an image with the graph                  |
| VDataFrame.to_pandas()                 | Convert to pandas dataframe                     |
| VDataFrame.to_csv()                    | Save to *glob* files                            |
| VDataFrame.to_excel()<sup>*</sup>      | Save to *glob* files                            |
| VDataFrame.to_feather()<sup>*</sup>    | Save to *glob* files                            |
| VDataFrame.to_hdf()<sup>*</sup>        | Save to *glob* files                            |
| VDataFrame.to_json()                   | Save to *glob* files                            |
| VDataFrame.to_orc()                    | Save to *glob* files                            |
| VDataFrame.to_parquet()                | Save to *glob* files                            |
| VDataFrame.to_sql()<sup>*</sup>        | Save to sql table                               |
| VDataFrame.to_numpy()                  | Convert to numpy array                          |
| VDataFrame.to_backend()                | Convert to backend dataframe                    |
| VDataFrame.categorize()                | Detect all categories                           |
| VDataFrame.apply_rows()                | Apply rows, GPU template                        |
| VDataFrame.map_partitions()            | Apply function for each parttions               |
| VSeries.compute()                      | Compute the virtual series                      |
| VSeries.persist()                      | Persist the dataframe in memory                 |
| VSeries.repartition()                  | Rebalance the dataframe                         |
| VSeries.visualize()                    | Create an image with the graph                  |
| VSeries.to_pandas()                    | Convert to pandas series                        |
| VSeries.to_backend()                   | Convert to backend series                       |
| VSeries.to_numpy()                     | Convert to numpy array                          |
| VClient(...)                           | The connexion with the cluster                  |
| VDF_MODE                               | The current mode                                |
| Mode                                   | The enumeration of differents mode              |

<sup>*</sup> some frameworks do not implement it, see below


You can read a sample notebook [here](https://github.com/pprados/virtual-dataframe/blob/master/notebooks/demo.ipynb)
for an exemple of all API.

Each API propose a specific version for each framework. For example:

- the  `toPandas()` with Panda, return `self`
- `@delayed` use the dask `@delayed` or do nothing, and apply the code when the function was called.
In the first case, the function return a part of the graph. In the second case, the function return immediately
the result.
- `read_csv("file*.csv")` read each file in parallele with dask by each worker,
and read sequencially each file, and combine each dataframe, with a framework without distribution (pandas, cudf)
- `save_csv("file*.csv")` write each file in parallele with dask, and write one file `"file.csv"` (without the star)
with a framework without distribution (pandas, cudf)
- ...

## Compatibility
This project is just a wrapper. So, it inherits limitations and bugs from other projects. Sorry for that.


| Limitations                                                                                     |
|-------------------------------------------------------------------------------------------------|
| <br />**pandas**                                                                                |
| All data must be in DRAM                                                                        |
| <br />**modin**                                                                                 |
| [Read this](https://modin.readthedocs.io/en/stable/getting_started/why_modin/pandas.html)       |
| <br />**[cudf](https://docs.rapids.ai/api/cudf/nightly/user_guide/pandas-comparison.html)**     |
| All data must be in VRAM                                                                        |
| All data types in cuDF are nullable                                                             |
| Iterating over a cuDF Series, DataFrame or Index is not supported.                              |
| Join (or merge) and groupby operations in cuDF do not guarantee output ordering.                |
| The order of operations is not always deterministic                                             |
| Cudf does not support duplicate column names                                                    |
| Cudf also supports .apply() it relies on Numba to JIT compile the UDF and execute it on the GPU |
| .apply(result_type=...) not supported                                                           |
| <br />**[dask](https://distributed.dask.org/en/stable/limitations.html)**                       |
|  transpose() and MultiIndex are not implemented                                                 |
| Column assignment doesn't support type list                                                     |
| <br />**dask-cudf**                                                                             |
| See cudf and dask.                                                                              |
| Categories with strings not implemented                                                         |

### File format compatibility
To be compatible with all framework, you must only use the common features.
We accept some function to read or write files, but we write a warning
if you use a function not compatible with others frameworks.

| read_... / to_...     | pandas | cudf | modin | dask | dask_cudf |
|-----------------------|:------:|:----:|:-----:|:----:|:---------:|
| vdf.read_csv          |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VDataFrame.to_csv     |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VSeries.to_csv        |   ✓    |      |   ✓   |  ✓   |     ✓     |
| vdf.read_excel        |   ✓    |      |   ✓   |      |           |
| VDataFrame.to_excel   |   ✓    |      |   ✓   |      |           |
| VSeries.to_excel      |   ✓    |      |   ✓   |      |           |
| vdf.read_feather      |   ✓    |  ✓   |   ✓   |      |           |
| VDataFrame.to_feather |   ✓    |  ✓   |   ✓   |      |           |
| vdf.read_fwf          |   ✓    |      |   ✓   |  ✓   |           |
| VDataFrame.to_fwf     |        |      |       |      |           |
| vdf.read_hdf          |   ✓    |  ✓   |   ✓   |  ✓   |           |
| VDataFrame.to_hdf     |   ✓    |  ✓   |   ✓   |  ✓   |           |
| VSeries.to_hdf        |   ✓    |  ✓   |   ✓   |  ✓   |           |
| vdf.read_json         |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VDataFrame.to_json    |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VSeries.to_json       |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| vdf.read_orc          |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VDataFrame.to_orc     |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| vdf.read_parquet      |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| VDataFrame.to_parquet |   ✓    |  ✓   |   ✓   |  ✓   |     ✓     |
| vdf.read_sql_table    |   ✓    |      |   ✓   |  ✓   |           |
| VDataFrame.to_sql     |   ✓    |      |   ✓   |  ✓   |           |
| VSeries.to_sql        |   ✓    |      |   ✓   |  ✓   |           |



### Cross framework compatibility

|       | small data         | middle data      | big data                        |
|-------|--------------------|------------------|---------------------------------|
| 1-CPU | pandas<br/>Limit:+ |                  |                                 |
| n-CPU |                    | modin<br/>Limit+ | dask or dask_modin<br/>Limit:++ |
| GPU   | cudf<br/>Limit:++  |                  | dask_cudf<br/>Limit:+++         |

To develop, you can choose the level to be compatible with others frameworks.
Each cell is strongly compatible with the upper left part.

### No need of GPU?
If you don't need to use a GPU, then develop for `dask` and use mode in *bold*.

|       | small data             | middle data           | big data                            |
|-------|------------------------|-----------------------|-------------------------------------|
| 1-CPU | **pandas<br/>Limit:+** |                       |                                     |
| n-CPU |                        | **modin<br/>Limite+** | **dask or dask_modin<br/>Limit:++** |
| GPU   | cudf<br/>Limit:++      |                       | dask_cudf<br/>Limit:+++             |

You can ignore this API:
- `VDataFrame.apply_rows()`

### No need of big data?

If you don't need to use big data, then develop for `cudf` and use mode in *bold*..

|       | small data             | middle data          | big data                        |
|-------|------------------------|----------------------|---------------------------------|
| 1-CPU | **pandas<br/>Limit:+** |                      |                                 |
| n-CPU |                        | **modin<br/>Limit+** | dask or dask_modin<br/>Limit:++ |
| GPU   | **cudf<br/>Limit:++**  |                      | dask_cudf<br/>Limit:+++         |

You can ignore these API:
- `@delayed`
- `map_partitions()`
- `categorize()`
- `compute()`
- `npartitions=...`

### Need all possibility?

To be compatible with all mode, develop for `dask_cudf` and use mode in *bold*..

|       | small data             | middle data          | big data                            |
|-------|------------------------|----------------------|-------------------------------------|
| 1-CPU | **pandas<br/>Limit:+** |                      |                                     |
| n-CPU |                        | **modin<br/>Limit+** | **dask or dask_modin<br/>Limit:++** |
| GPU   | **cudf<br/>Limit:++**  |                      | **dask_cudf<br/>Limit:+++**         |

and accept all the limitations.


## Best practices

For write a code, optimized with all frameworks, you must use some *best practices*.

### Use *read file*
It's not a good idea to use the constructor of `VDataFrame` or `VSeries` because, all the datas
must be in memory in the driver. If the dataframe is big, an *out of memory* can happen.
To partitionning the job, create multiple files, and use `read_csv("filename*.csv")` or other
file format. Then, each worker can read directly a partition.

The constructor for `VDataFrame` and `VSeries` are present, only to help to write some unit test,
but may not be used in production.

### No loop
It's not a good idea to iterate over row of DataFrame. Some framework are very slow with this approach.
It's better to use `apply()` or `apply_rows()` to distribute the code in the cluster and GPU.
The code used in `apply`, will be compiled to CPU or GPU, before using, with some frameworks.


## FAQ

### The code run with dask, but not with modin, pandas or cudf ?
You must use only the similare functionality, and only a subpart of Pandas.
Develop for *dask_cudf*. it's easier to be compatible with others frameworks.

### `.compute()` is not defined with pandas, cudf ?
If your `@delayed` function return something, other than a `VDataFrame` or `VSerie`, the objet has not
the method `.compute()`. You can solve this, with:
```
@delayed
def f()-> int:
    return 42

real_result,=compute(f())  # Warning, compute return a tuple. The comma is important.
a,b = compute(f(),f())
```

