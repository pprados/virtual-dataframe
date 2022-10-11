## Cluster
To connect to a cluster, use `VDF_CLUSTER` with protocol, host and optionaly, the port.

- `dask://locahost:8787`
- or alternativelly, use `DASK_SCHEDULER_SERVICE_HOST` and `DASK_SCHEDULER_SERVICE_PORT`

| VDF_MODE   | DEBUG | VDF_CLUSTER                | Scheduler        |
|------------|-------|----------------------------|------------------|
| pandas     | -     | -                          | No scheduler     |
| cudf       | -     | -                          | No scheduler     |
| modin      | -     | -                          | No scheduler     |
| dask       | Yes   | -                          | synchronous      |
| dask       | No    | -                          | thread           |
| dask       | No    | dask://threads             | thread           |
| dask       | No    | dask://processes           | processes        |
| dask       | No    | dask://.local              | LocalCluster     |
| dask_modin | No    | -                          | LocalCluster     |
| dask_modin | No    | dask://.local              | LocalCluster     |
| dask_modin | No    | dask://&lt;host>:&lt;port> | Dask cluster     |
| dask_cudf  | No    | dask://.local              | LocalCUDACluster |
| dask_cudf  | No    | dask://&lt;host>:&lt;port> | Dask cluster     |


The special *host name*, ends with `.local` can be use to start a `LocalCluster` ( or `LocalCUDACluster` ) when
your program is started. An instance of local cluster is started and injected in the `Client`.
Then, your code can not manage the local cluster.

To update the parameters for the *implicit* `Local(CUDA)Cluster`, you can use the
[Dask config file](https://docs.dask.org/en/stable/configuration.html).

```yaml
local:
  scheduler-port: 0
  device_memory_limit: 5G
```

or you can set some environment variables.
```shell
export DASK_LOCAL__SCHEDULER_PORT=0
export DASK_LOCAL__DEVICE_MEMORY_LIMIT=5g
```

Sample:
```
from virtual_dataframe import VClient

with (VClient())
    # Now, use the scheduler
```

If you want to manage the parameters of `Local(CUDA)Cluster`, use the alternative `VLocalCluster`.
```
from virtual_dataframe import VClient,VLocalCluster

with (VClient(VLocalCluster()))
    # Now, use the scheduler
```
