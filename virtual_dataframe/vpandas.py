"""
Virtual Dataframe and Series.
"""
# flake8: noqa
import glob
from functools import wraps
from typing import Any, List, Tuple, Optional, Union

from pandas._typing import Axes, Dtype

from .env import VDF_MODE, Mode

_doc_delayed = ''' Fake @dask.delayed. Do nothing.'''
_doc_from_pandas = '''
Construct a VDataFrame from a Pandas DataFrame

    Return data

    Parameters
    ----------
        data :  pandas.DataFrame or pandas.Series
                The DataFrame/Series with which to construct a Dask DataFrame/Series
        npartitions : int, optional
                ignored
        chunksize : int, optional
                ignored
        sort: bool
                ignored
        name: string, optional
                ignored

        Returns
        -------
            VDataFrame or VSeries
                A dask DataFrame/Series
'''

_doc_from_virtual = '''Convert VDataFrame to VDataFrame'''
_doc_VDataFrame_to_csv = '''Convert CSV files to VDataFrame'''
_doc_VSeries_to_csv = '''Convert CSV files to VSeries'''
_doc_VDataFrame_to_pandas = '''Convert VDataFrame to Pandas DataFrame'''
_doc_VSeries_to_pandas = '''Convert VSeries to Pandas DataFrame'''
_doc_VDataFrame_to_numpy = '''Convert VDataFrame to Numpy array'''
_doc_VSeries_to_numpy = '''Convert VSeries to Numpy array'''
_doc_VDataFrame_compute = '''Fake compute(). Return self.'''
_doc_categorize = '''
Convert columns of the DataFrame to category dtype.

        Parameters
        ----------
            columns : list, optional
                           A list of column names to convert to categoricals. By default any
                           column with an object dtype is converted to a categorical, and any
                           unknown categoricals are made known.
            index : bool, optional
                           Whether to categorize the index. By default, object indices are
                           converted to categorical, and unknown categorical indices are made
                           known. Set True to always categorize the index, False to never.
            split_every : int, optional
                          Group partitions into groups of this size while performing a
                          tree-reduction. If set to False, no tree-reduction will be used.
                          Default is 16.
            kwargs
                          Keyword arguments are passed on to compute.'''
_doc_compute = '''Compute several dask collections at once. Return args.

    Parameters
    ----------
        args : object
                Any number of objects. If it is a dask object, it\'s computed and the
                result is returned. By default, python builtin collections are also
                traversed to look for dask objects (for more information see the
                ``traverse`` keyword). Non-dask arguments are passed through unchanged.
        traverse : bool, optional
                By default dask traverses builtin python collections looking for dask
                objects passed to ``compute``. For large collections this can be
                expensive. If none of the arguments contain any dask objects, set
                ``traverse=False`` to avoid doing this traversal.
        scheduler : string, optional
                Which scheduler to use like "threads", "synchronous" or "processes".
                If not provided, the default is to check the global settings first,
                and then fall back to the collection defaults.
        optimize_graph : bool, optional
                If True [default], the optimizations for each collection are applied
                before computation. Otherwise the graph is run as is. This can be
                useful for debugging.
        get : ``None``
                Should be left to ``None`` The get= keyword has been removed.
        kwargs
                Extra keywords to forward to the scheduler function.
'''


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

    def _remove_dask_parameters(func, *part_args, **kwargs):
        return _remove_parameters(func, ["npartitions", "chunksize", "sort", "name"])


    """
    Fake of `@dask.delayed`. Do nothing.
    """


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

    _from_back: Any = dask_cudf.from_cudf

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_virtual: Any = dask_cudf.from_cudf

    read_csv: Any = dask.dataframe.read_csv

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series

    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

# %%
if VDF_MODE == Mode.dask:
    import pandas
    import dask

    _BackDataFrame: Any = pandas.DataFrame
    _BackSeries: Any = pandas.Series

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_virtual: Any = dask.dataframe.from_pandas

    read_csv: Any = dask.dataframe.read_csv

    _from_back: Any = dask.dataframe.from_pandas

    # pandas.core.frame.DataFrame.to_pandas = lambda self: self
    _BackDataFrame.to_pandas = lambda self: self
    _BackDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _BackSeries.to_pandas = lambda self: self
    _BackSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

# %%
if VDF_MODE == Mode.cudf:
    import cudf
    import pandas

    _BackDataFrame: Any = cudf.DataFrame
    _BackSeries: Any = cudf.Series

    _VDataFrame: Any = cudf.DataFrame
    _VSeries: Any = cudf.Series

    '''Fake @dask.delayed'''
    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed

    concat: Any = cudf.concat


    def _read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return cudf.concat((cudf.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return cudf.read_csv(filepath_or_buffer, **kwargs)


    read_csv: Any = _read_csv
    read_csv.__doc__ = cudf.read_csv.__doc__

    from_pandas: Any = _remove_dask_parameters(cudf.from_pandas)
    from_pandas.__doc__ = _doc_from_pandas
    from_virtual: Any = _remove_dask_parameters(lambda self: self)
    from_virtual.__doc__ = _doc_from_virtual

    # Add fake compute() in cuDF
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VDataFrame_compute

    _old_DataFrame_to_csv = _VDataFrame.to_csv


    def _DataFrame_to_csv(self, filepath_or_buffer, **kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*", "")
        return _old_DataFrame_to_csv(self, filepath_or_buffer, **kwargs)


    # _old_Series_to_csv = _VSeries.to_csv
    # def _Series_to_csv(self, filepath_or_buffer, **kwargs):
    #     if "*" in str(filepath_or_buffer):
    #         filepath_or_buffer = filepath_or_buffer.replace("*", "")
    #     return _old_Series_to_csv(self, filepath_or_buffer, **kwargs)

    _VDataFrame.to_csv = _remove_to_csv(_DataFrame_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv
    # _VSeries.to_csv = _remove_to_csv(_Series_to_csv)
    # _VSeries.to_csv.__doc__ = _doc_VSeries_to_csv

    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize


    # noinspection PyUnusedLocal
    def compute(*args,
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> List:
        return list(args)


    compute.__doc__ = _doc_compute


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

    # _BackDataFrame = pandas.DataFrame
    # _BackSeries = pandas.Series
    # _BackIndex = pandas.Index
    _BackDataFrame: Any = pandas.DataFrame
    _BackSeries: Any = pandas.Series

    _VDataFrame: Any = pandas.DataFrame
    _VSeries: Any = pandas.Series


    # def delayed(name: Optional[str] = None,
    #             pure: Optional[bool] = None,
    #             nout: Optional[int] = None,
    #             traverse: Optional[bool] = True) -> Any:
    #     _delayed(name, pure, nout, traverse)

    # noinspection PyUnusedLocal
    def _from_back(  # noqa: F811
            data: Union[_BackDataFrame, _BackSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed

    concat: Any = pandas.concat


    # noinspection PyUnusedLocal
    def compute(*args,  # noqa: F811
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> Tuple:
        return args


    compute.__doc__ = _doc_compute


    def read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return pandas.concat((pandas.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return pandas.read_csv(filepath_or_buffer, **kwargs)


    read_csv.__doc__ = pandas.read_csv.__doc__

    from_pandas: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_pandas.__doc__ = _doc_from_pandas
    from_virtual: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_virtual.__doc__ = _doc_from_virtual

    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VDataFrame_compute

    # Add fake to_pandas() in pandas
    _VDataFrame.to_pandas = lambda self: self
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_pandas = lambda self: self
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _old_DataFrame_to_csv = _VDataFrame.to_csv


    def _DataFrame_to_csv(self, filepath_or_buffer, **kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*", "")
        return _old_DataFrame_to_csv(self, filepath_or_buffer, **kwargs)


    _old_Series_to_csv = _VSeries.to_csv


    def _Series_to_csv(self, filepath_or_buffer, **kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*", "")
        return _old_Series_to_csv(self, filepath_or_buffer, **kwargs)


    _VDataFrame.to_csv = _remove_to_csv(_DataFrame_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv
    # _VSeries.to_csv = _remove_to_csv(_Series_to_csv)
    # _VSeries.to_csv.__doc__ = _doc_VSeries_to_csv
    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize

# %%
''' A *virtual* dataframe.
The concret dataframe depend on the environment variable `VDF_MODE`.
It's may be : `pandas.DataFrame`, `cudf.DataFrame` or `dask.DataFrame`
'''


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


''' A *virtual* series.
The concret series depend on the environment variable `VDF_MODE`.
It's may be : `pandas.Series`, `cudf.Series` or `dask.Series`
'''


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
