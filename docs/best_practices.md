## Best practices

For write a code, optimized with all frameworks, you must use some *best practices*.

### Use *read file*
It's not a good idea to use the constructor of `VDataFrame` or `VSeries` because, all the datas
must be in memory in the driver. If the dataframe is big, an *out of memory* can happen.
To partitionning the job, create multiple files, and use `read_csv("filename*.csv")`,
use a big file and the *auto partitioning* proposed by dask
or other file format. Then, each worker can read directly a partition.

The constructor for `VDataFrame` and `VSeries` are present, only to help to write some unit test,
but may not be used in production.

### No loop
It's not a good idea to iterate over row of DataFrame. Some framework are very slow with this approach.
It's better to use `apply()` or `apply_rows()` to distribute the code in the cluster and GPU.
The code used in `apply`, will be compiled to CPU or GPU, before using, with some frameworks.


