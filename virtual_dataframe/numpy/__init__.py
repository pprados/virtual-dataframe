import sys
from typing import Tuple, List

from virtual_dataframe import VDF_MODE, Mode


def _remove_args(func, _params: List[str]):
    def wrapper(*args, **kwargs):
        for k in _params:
            kwargs.pop(k, None)
        rc = func(*args, **kwargs)
        return rc

    return wrapper


def _compute(*args,  # noqa: F811
             **kwargs
             ) -> Tuple:
    return tuple(args)


if VDF_MODE in (Mode.pandas, Mode.numpy, Mode.modin, Mode.dask_modin, Mode.pyspark, Mode.pyspark_gpu):

    import numpy

    FrontEndNumpy = numpy

    sys.modules[__name__] = FrontEndNumpy  # Hack to replace this current module to another

    FrontEndNumpy.asnumpy = lambda a: a
    FrontEndNumpy.asndarray = lambda a: a.to_numpy()
    FrontEndNumpy.compute = _compute
    FrontEndNumpy.compute_chunk_sizes = lambda a: a
    FrontEndNumpy.rechunk = lambda a, *args, **kwargs: a
    FrontEndNumpy.arange = _remove_args(FrontEndNumpy.arange, ['chunks'])
    FrontEndNumpy.from_array = _remove_args(FrontEndNumpy.array, ["chunks"])
    FrontEndNumpy.load = _remove_args(FrontEndNumpy.load, ["chunks"])
    FrontEndNumpy.save = _remove_args(FrontEndNumpy.save, ["chunks"])
    FrontEndNumpy.savez = _remove_args(FrontEndNumpy.savez, ["chunks"])

    FrontEndNumpy.random.random = _remove_args(FrontEndNumpy.random.random, ["chunks"])
    FrontEndNumpy.random.binomial = _remove_args(FrontEndNumpy.random.binomial, ["chunks"])
    FrontEndNumpy.random.normal = _remove_args(FrontEndNumpy.random.normal, ["chunks"])
    FrontEndNumpy.random.poisson = _remove_args(FrontEndNumpy.random.poisson, ["chunks"])

elif VDF_MODE in (Mode.cudf, Mode.cupy):
    import cupy

    FrontEndNumpy = cupy

    sys.modules[__name__] = FrontEndNumpy  # Hack to replace this current module to another

    FrontEndNumpy.asndarray = lambda a: a.to_cupy()
    FrontEndNumpy.compute = _compute
    FrontEndNumpy.compute_chunk_sizes = lambda a: a
    FrontEndNumpy.rechunk = lambda a, *args, **kwargs: a
    FrontEndNumpy.arange = _remove_args(FrontEndNumpy.arange, ['chunks'])
    FrontEndNumpy.from_array = _remove_args(FrontEndNumpy.array, ["chunks"])
    FrontEndNumpy.load = _remove_args(FrontEndNumpy.load, ["chunks"])
    FrontEndNumpy.save = _remove_args(FrontEndNumpy.save, ["chunks"])
    FrontEndNumpy.savez = _remove_args(FrontEndNumpy.savez, ["chunks"])

    FrontEndNumpy.random.random = _remove_args(FrontEndNumpy.random.random, ["chunks"])
    FrontEndNumpy.random.binomial = _remove_args(FrontEndNumpy.random.binomial, ["chunks"])
    FrontEndNumpy.random.normal = _remove_args(FrontEndNumpy.random.normal, ["chunks"])
    FrontEndNumpy.random.poisson = _remove_args(FrontEndNumpy.random.poisson, ["chunks"])

if VDF_MODE in (Mode.pyspark, Mode.pyspark_gpu):
    import pyspark
    import numpy

    _old_asarray = numpy.asarray


    def _asarray(
            a, dtype=None, order=None, **kwargs
    ):
        if isinstance(a, (pyspark.pandas.series.DataFrame, pyspark.pandas.series.Series)):
            return _old_asarray(a.to_numpy(),
                                dtype=dtype,
                                order=order,
                                **kwargs)
        else:
            return _old_asarray(a,
                                dtype=dtype,
                                order=order,
                                **kwargs)


    FrontEndNumpy.asarray = _asarray
    from_array = _remove_args(numpy.array, ["chunks"])
    load = _remove_args(numpy.load, ["chunks"])
    save = _remove_args(numpy.save, ["chunks"])
    savez = _remove_args(numpy.savez, ["chunks"])

if VDF_MODE in (Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy):

    import dask.array
    import numpy

    FrontEndNumpy = dask.array

    sys.modules[__name__] = dask.array  # Hack to replace this current module to another

    if VDF_MODE in (Mode.dask_cudf, Mode.dask_cupy):
        import cupy


        def _asnumpy(df):
            if isinstance(df, cupy.ndarray):
                return cupy.asnumpy(df)
            elif isinstance(df, numpy.ndarray):
                return df
            return cupy.asnumpy(df.compute())
    else:
        def _asnumpy(df):
            if isinstance(df, numpy.ndarray):
                return df
            return df.compute()
    FrontEndNumpy.asnumpy = _asnumpy

    if VDF_MODE not in (Mode.dask_cudf, Mode.dask_cupy):
        FrontEndNumpy.asndarray = lambda df: df.compute().to_numpy()
    else:
        FrontEndNumpy.asndarray = lambda df: df.compute().to_cupy()
    # _old_asarray = dask.array.asarray

    FrontEndNumpy.load = _remove_args(dask.array.from_npy_stack, ["chunks", "allow_pickle"])
    FrontEndNumpy.save = _remove_args(dask.array.to_npy_stack, ["chunks", "allow_pickle"])
    FrontEndNumpy.compute_chunk_sizes = lambda a: a.compute_chunk_sizes()


def rechunk(arr, *args, **kwargs):
    return arr


def compute(*args,  # noqa: F811
            **kwargs
            ) -> Tuple:
    return tuple(args)
