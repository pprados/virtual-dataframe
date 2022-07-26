"""
"""
from functools import wraps
from typing import Any, List, Tuple, Optional, Union

from pandas._typing import Axes, Dtype

import pandera
from .env import VDF_MODE, Mode


# %%

def _remove_parameters(func, _params: List[str], *part_args, **kwargs):
    def wrapper(*args, **kwargs):
        for k in _params:
            kwargs.pop(k, None)
        return func(*args, **kwargs)

    return wrapper


def _remove_to_csv(func, *part_args, **kwargs):
    return _remove_parameters(func,
                              ["single_file",
                               "name_function",
                               "compute",
                               "scheduler",
                               "header_first_partition_only",
                               "compute_kwargs"])

if VDF_MODE in (Mode.pandas, Mode.cudf):
    """
    Fake of `delayed`, if dask is not used.
    """


    def _remove_dask_parameters(func, *part_args, **kwargs):
        return _remove_parameters(func, ["npartitions", "chunksize", "sort", "name"])


    def _delayed(name: Optional[str] = None,
                 pure: Optional[bool] = None,
                 nout: Optional[int] = None,
                 traverse: Optional[bool] = True):
        if callable(name):
            fun = name

            @wraps(fun)
            def wrapper(*args, **kwargs):
                return fun(*args, **kwargs)

            return wrapper
        else:
            def decorate(fun):
                @wraps(fun)
                def wrapper(*args, **kwargs):
                    return fun(*args, **kwargs)

                return wrapper

            return decorate

# %%
if VDF_MODE == Mode.dask_cudf:
    import pandas
    import dask
    import dask.distributed
    import dask_cudf
    import cudf  # See https://docs.rapids.ai/api/dask-cuda/nightly/install.html

    _BackDataFrame: Any = cudf.DataFrame
    _BackSeries: Any = cudf.Series

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series

    _VDataFrame.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas = lambda self: self.compute()

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_virtual: Any = dask_cudf.from_cudf

    read_csv: Any = dask.dataframe.read_csv

    _from_back: Any = dask_cudf.from_cudf

# %%
if VDF_MODE == Mode.dask:
    import pandas  # noqa: F811
    import dask

    _BackDataFrame: Any = pandas.DataFrame
    _BackSeries: Any = pandas.Series

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series

    # pandas.core.frame.DataFrame.to_pandas = lambda self: self
    _BackDataFrame.to_pandas = lambda self: self
    _BackSeries.to_pandas = lambda self: self

    _VDataFrame.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas = lambda self: self.compute()

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_virtual: Any = dask.dataframe.from_pandas

    read_csv: Any = dask.dataframe.read_csv

    _from_back: Any = dask.dataframe.from_pandas

# %%
if VDF_MODE == Mode.cudf:
    import cudf
    import pandas
    import dask_cudf
    import glob

    _BackDataFrame: Any = cudf.DataFrame
    _BackSeries: Any = cudf.Series

    _VDataFrame: Any = cudf.DataFrame
    _VSeries: Any = cudf.Series

    # Add fake delayed
    delayed: Any = _delayed

    concat: Any = cudf.concat

    from_pandas: Any = _remove_dask_parameters(cudf.from_pandas)
    from_virtual: Any = _remove_dask_parameters(lambda self: self)


    def _read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return cudf.concat([cudf.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)])
        else:
            return cudf.read_csv(filepath_or_buffer, **kwargs)


    read_csv:Any= _read_csv

    # Add fake compute() in cuDF
    _VDataFrame.compute = lambda self, **kwargs: self
    _VSeries.compute = lambda self, **kwargs: self

    _old_to_csv = _VDataFrame.to_csv
    def _to_csv(self, filepath_or_buffer,**kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*","")
        return _old_to_csv(self, filepath_or_buffer,**kwargs)
    _VDataFrame.to_csv = _remove_to_csv(_to_csv)

    _VDataFrame.categorize = lambda self: self


    # noinspection PyUnusedLocal
    def compute(*args,
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> List:
        return list(args)


    def _from_back(
            data: Union[_BackDataFrame, _BackSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data

# %%
if VDF_MODE == Mode.pandas:
    import pandas
    import glob

    # _BackDataFrame = pandas.DataFrame
    # _BackSeries = pandas.Series
    # _BackIndex = pandas.Index
    _BackDataFrame: Any = pandera.typing.DataFrame
    _BackSeries: Any = pandera.typing.Series

    _VDataFrame: Any = pandas.DataFrame
    _VSeries: Any = pandas.Series

    # Add Fake delayed
    delayed: Any = _delayed

    concat: Any = pandas.concat


    def _read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return pandas.concat([pandas.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)])
        else:
            return pandas.read_csv(filepath_or_buffer, **kwargs)


    read_csv:Any= _read_csv

    from_pandas: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_virtual: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df

    # Add fake compute() in pandas
    _VDataFrame.compute = lambda self, **kwargs: self
    _VSeries.compute = lambda self, **kwargs: self

    # Add fake to_pandas() in pandas
    _VDataFrame.to_pandas = lambda self: self
    _VSeries.to_pandas = lambda self: self

    _old_to_csv = _VDataFrame.to_csv
    def _to_csv(self, filepath_or_buffer,**kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*","")
        return _old_to_csv(self, filepath_or_buffer,**kwargs)
    _VDataFrame.to_csv = _remove_to_csv(_to_csv)
    _VDataFrame.categorize = lambda self: self


    # noinspection PyUnusedLocal
    def compute(*args,  # noqa: F811
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> Tuple:
        return args


    # noinspection PyUnusedLocal
    def _from_back(  # noqa: F811
            data: Union[_BackDataFrame, _BackSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


# %%
class VDataFrame(_VDataFrame):
    def __new__(cls,
                data=None,
                index: Optional[Axes] = None,
                columns: Optional[Axes] = None,
                dtype: Optional[Dtype] = None,

                npartitions: int = 1,
                chunksize: Optional[int] = None,
                sort: bool = True,
                name: Optional[str] = None,
                ) -> _VDataFrame:
        return _from_back(  # FIXME _remove_dask_parameters ?
            _BackDataFrame(data=data, index=index, columns=columns, dtype=dtype),
            npartitions=npartitions,
            chunksize=chunksize,
            sort=sort,
            name=name)


class VSeries(_VSeries):
    def __new__(cls,
                data=None,
                index: Optional[Axes] = None,
                dtype: Optional[Dtype] = None,

                npartitions: int = 1,
                chunksize: Optional[int] = None,
                sort: bool = True,
                name: Optional[str] = None,
                ) -> _VSeries:
        return _from_back(
            _BackSeries(data=data, index=index, dtype=dtype, name=name),
            npartitions=npartitions,
            chunksize=chunksize,
            sort=sort,
            name=name)


# %%
__all__: List[str] = ['VDF_MODE', 'Mode',
                      'VDataFrame', 'VSeries',
                      'delayed', 'compute',
                      ]
