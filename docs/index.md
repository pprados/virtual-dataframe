## Motivation

With Panda like dataframe, do you want to create a code, and choose at the end, the framework to use?
Do you want to be able to choose the best framework after simply performing performance measurements?
This framework unifies multiple Panda-compatible components, to allow the writing of a single code, compatible with all.

## Synopsis

With some parameters and Virtual classes, it's possible to write a code, and execute this code:

- With or without multicore
- With or without cluster (multi nodes on Dask or Spark)
- With or without GPU

To do that, we create some virtual classes, add some methods in others classes, etc.

To reduce the confusion, you must use the classes `VDataFrame` and `VSeries` (The prefix `V` is for *Virtual*).
These classes propose the methods `.to_pandas()` and `.compute()` for each version, but are the *real* classes
of the selected framework.

With some parameters, the real classes may be `pandas.DataFrame`, `modin.pandas.DataFrame`,
`cudf.DataFrame`,
`pyspark.pandas.DataFrame` without GPU,
`pyspark.pandas.DataFrame` with GPU,
`dask.dataframe.DataFrame` with Pandas or
`dask.dataframe.DataFrame` with cudf (with Pandas or cudf for each partition).

A new `@delayed` annotation can be use, with or without Dask.

To manage the initialisation of a Dask ou Spark, you must use the `VClient()`,
a connector to the cluster.
This alias, can be automatically initialized with some environment variables.

```python
# Sample of code, compatible Pandas, cudf, dask, dask_modin and dask_cudf
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

| env                                                 | Environement                                |
|-----------------------------------------------------|---------------------------------------------|
| VDF_MODE=pandas                                     | Only Python with classical pandas           |
| VDF_MODE=cudf                                       | Python with local cuDF (GPU)                |
| VDF_MODE=dask                                       | Dask with local multiple process and pandas |
| VDF_MODE=dask_cudf                                  | Dask with local multiple process and cuDF   |
| VDF_MODE=dask<br />DEBUG=True                       | Dask with single thread and pandas          |
| VDF_MODE=dask_cudf<br />DEBUG=True                  | Dask with single thread and cuDF            |
| VDF_MODE=dask<br />VDF_CLUSTER=dask://.local        | Dask with local cluster and pandas          |
| VDF_MODE=dask_cudf<br />VDF_CLUSTER=dask://.local   | Dask with local cuda cluster and cuDF       |
| VDF_MODE=dask<br />VDF_CLUSTER=dask://...:ppp       | Dask with remote cluster and Pandas         |
| VDF_MODE=dask_cudf<br />VDF_CLUSTER=dask://...:ppp  | Dask with remote cluster and cuDF           |
| VDF_MODE=dask_modin<br />                           | Dask with modin                             |
| VDF_MODE=dask_modin<br />VDF_CLUSTER=dask://.local  | Dask with local cluster and modin           |
| VDF_MODE=dask_modin<br />VDF_CLUSTER=dask://...:ppp | Dask with remote cluster and modin          |
| VDF_MODE=pyspark<br />VDF_CLUSTER=spark://.local    | PySpark with local cluster and modin        |
| VDF_MODE=pyspark<br />VDF_CLUSTER=spark://...:ppp   | PySpark with remote cluster and modin       |

*For pyspark with GPU, read [this](cluster.md).*

The real compatibilty between the differents simulation of Pandas, depends on the implement of the modin, cudf, pyspark
or dask. Sometime, you can use the `VDF_MODE` variable, to update some part of code, between
the selected backend.

It's not always easy to write a code *compatible* with all scenario, but it's possible.
Generally, add just `.compute()` and/or `.to_pandas()` at the end of the ETL, is enough.
But, you must use, only the common feature with all frameworks.
After this effort, it's possible to compare the performance about the differents technologies,
or propose a component, compatible with differents scenario.

For the deployment of your project, you can select the best framework for your process
(in a dockerfile? or virtual environment),
with only one ou two environment variables.

With conda environment, you can use [variables](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#setting-environment-variables).
