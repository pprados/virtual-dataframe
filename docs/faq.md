## FAQ

### The code run with X, but not with Y?
You must use only the similar functionality, and only a subpart of Pandas.
Develop for *dask_cudf*. it's easier to be compatible with others frameworks.

### `.compute()` is not defined with pandas, cudf?
If your `@delayed` function return something, other than a `VDataFrame` or `VSerie`, the objet does not have
the method `.compute()`. You can solve this, with:
```
@delayed
def f()-> int:
    return 42

real_result,=compute(f())  # Warning, compute return a tuple. The comma is important.
a,b = compute(f(),f())
```

### With CUDA, I receive `NVMLError_NoPermission`
It's a problem with Dask. You have not the privilege to ask the size of memory for the GPU.
To resolve that, add in dask [configuration files](https://docs.dask.org/en/stable/configuration.html):

- the parameter `distributed:diagnostics:nvml = False`
- the parameter `local:device_memory_limit = 5g` or the parameter `--device-memory-limit 5g` when you start
`dask-cuda-worker` (update the size)
