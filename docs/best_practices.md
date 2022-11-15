## Best practices

For write a code, optimized with all frameworks, you must use some *best practices*.

- **Avoid very large partition:** so that they fit in a worker's available memory.
- **Avoid very large graphs:** because that can create an overhead on task.
- **Learn techniques for customization:** in order to improve the efficiency of your processes.
- **Stop using Dask when no longer needed:** like when you are iterating on a much smaller amount of data.
- **Persist in dsitributed RAM when you can:** in doing so, accessing RAM memory will be faster.
- **Processes and threads:** be careful to separate numeric work from text data to maintain efficiency.
- **Load data with Dask or Spark**: for instance, if you need to work with large Python objects, let Dask create them
  (instead of creating them outside Dask or Spark and then handing thom over the framework).
- **Avoid using `VDataFrame` or `VSeries`** outside the unit tests
- **Avoid calling compute repeatedly:** as this can lower performance.
- **Avoid iterate over rows or columns of DataFrame:** Use `apply()` or `apply_rows()` to distribute the code in
the cluster and GPU.


If you know that the volume of data is compatible with one node, you can convert a distributed dataframe
to `BackendDataFrame` and continue to manipulate the data locally.
Use the `.to_backend()` to convert a `Dataframe` to Pandas or cudf Dataframe.
