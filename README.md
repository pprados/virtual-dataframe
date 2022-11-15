# Virtual DataFrame

[Full documentation](https://pprados.github.io/virtual_dataframe/)

## Motivation

With Panda-like dataframe, do you want to create a code, and choose at the end, the framework to use?
Do you want to be able to choose the best framework after simply performing performance measurements?
This framework unifies multiple Panda-compatible components, to allow the writing of a single code, compatible with all.

## Synopsis

With some parameters and Virtual classes, it's possible to write a code, and execute this code:

- With or without multicore
- With or without cluster (multi nodes)
- With or without GPU

To do that, we create some virtual classes, add some methods in others classes, etc.

It's difficult to use a combinaison of framework, with the same classe name, with similare semantic, etc.
For example, if you want to use in the same program, Dask, cudf, pandas, modin, pyspark or pyspark+rapids,
you must manage:

- `pandas.DataFrame`, `pandas,Series`
- `modin.pandas.DataFrame`, `modin.pandas.Series`
- `cudf.DataFrame`, `cudf.Series`
- `dask.DataFrame`, `dask.Series`
- `pyspark.pandas.DataFrame`, `pyspark.pandas.Series`

 With `cudf`, the code must call `.to_pandas()`. With dask, the code must call `.compute()`, can use `@delayed` or
`dask.distributed.Client`. etc.

We propose to replace all these classes and scenarios, with a *uniform model*,
inspired by [dask](https://www.dask.org/) (the more complex API).
Then, it is possible to write one code, and use it in differents environnements and frameworks.

This project is essentially a back-port of *Dask+Cudf* to others frameworks.
We try to normalize the API of all frameworks.

