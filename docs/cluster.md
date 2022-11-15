## Cluster
To connect to a cluster, use `VDF_CLUSTER` with protocol, host and optionaly, the port.

- `dask://locahost:8787`
- `spark://locahost:7077`
- or alternativelly,
  - use `DASK_SCHEDULER_SERVICE_HOST` and `DASK_SCHEDULER_SERVICE_PORT`
  - or `SPARK_MASTER_HOST` and `SPARK_MASTER_PORT`

| VDF_MODE   | DEBUG | VDF_CLUSTER                 | Scheduler           |
|------------|-------|-----------------------------|---------------------|
| pandas     | -     | -                           | No scheduler        |
| cudf       | -     | -                           | No scheduler        |
| modin      | -     | -                           | No scheduler        |
| dask       | Yes   | -                           | synchronous         |
| dask       | No    | -                           | thread              |
| dask       | No    | dask://threads              | thread              |
| dask       | No    | dask://processes            | processes           |
| dask       | No    | dask://.local               | LocalCluster        |
| dask_modin | No    | -                           | LocalCluster        |
| dask_modin | No    | dask://.local               | LocalCluster        |
| dask_modin | No    | dask://&lt;host>:&lt;port>  | Dask cluster        |
| dask_cudf  | No    | dask://.local               | LocalCUDACluster    |
| dask_cudf  | No    | dask://&lt;host>:&lt;port>  | Dask cluster        |
| pyspark    | No    | spark:local[*]              | Spark local cluster |
| pyspark    | No    | spark://.local              | Spark local cluster |
| pyspark    | No    | spark://&lt;host>:&lt;port> | Spark cluster       |


The special *host name*, ends with `.local` can be used to start a `LocalCluster`,
`LocalCUDACluster` or Spark `local[*]` when your program is started.
An instance of local cluster is started and injected in the `Client`.

Sample:
```
from virtual_dataframe import VClient

with VClient():
    # Now, use the scheduler
    pass
```

If you want to manage the parameters of `Local(CUDA)Cluster` or `SparkCluster`,
use the alternative `VLocalCluster()`.
```
from virtual_dataframe import VClient,VLocalCluster

with VClient(VLocalCluster(params=...)):
    # Now, use the scheduler
    pass
```

## Dask local cluster
To update the parameters for the *implicit* `Local(CUDA)Cluster`,

- you can use the
[Dask config file](https://docs.dask.org/en/stable/configuration.html).

```yaml
local:
  scheduler-port: 0
  device_memory_limit: 5G
```

- you can set some environment variables for dask,
```shell
export DASK_LOCAL__SCHEDULER_PORT=0
export DASK_LOCAL__DEVICE_MEMORY_LIMIT=5g
```

- or for Domino datalab,
```shell
export DASK_SCHEDULER_SERVICE_HOST=...
export DASK_SCHEDULER_SERVICE_PORT=7077
```

## Spark cluster
To configure the spark cluster,
- use a file `spark.conf` with the
[Spark properties](https://spark.apache.org/docs/2.1.0/configuration.html#spark-properties)

- use environment variables like `export spark.app.name=MyApp`.

- for `VLocalCluster`, use classical parameters, and replace dot to `_`:
```
from virtual_dataframe import VClient,VLocalCluster

with VClient(VLocalCluster(
        spark_app_name="MyApp",
        spark_master="local[*]",
    )):
    # Now, use the scheduler
    pass
```


- or for Domino datalab,
```shell
export SPARK_MASTER_HOST=...
export SPARK_MASTER_PORT=7077
```

## Spark cluster with GPU
To use the [Spark+rapids](https://nvidia.github.io/spark-rapids/), download the file
[`rapids-4-spark_2.12-22.10.0.jar`](https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.10.0/rapids-4-spark_2.12-22.10.0.jar)
(see [here](https://nvidia.github.io/spark-rapids/docs/download.html)).

Then, in the file `spark.conf`, add:

```
spark.jars=rapids-4-spark_2.12-22.10.0.jar
spark.plugins=com.nvidia.spark.SQLPlugin
spark.rapids.sql.concurrentGpuTasks=1
```


