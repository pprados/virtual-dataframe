"""
Virtual Dataframe and Series.
"""
# flake8: noqa
import glob
import sys
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

_doc_apply_rows = '''
apply_rows(func, incols, outcols, kwargs, pessimistic_nulls=True, cache_key=None) method of cudf.core.dataframe.DataFrame instance
    Apply a row-wise user defined function.
    See https://docs.rapids.ai/api/cudf/stable/user_guide/guide-to-udfs.html#lower-level-control-with-custom-numba-kernels

    Parameters
    ----------
    df : DataFrame
        The source dataframe.
    func : function
        The transformation function that will be executed on the CUDA GPU.
    incols: list or dict
        A list of names of input columns that match the function arguments.
        Or, a dictionary mapping input column names to their corresponding
        function arguments such as {'col1': 'arg1'}.
    outcols: dict
        A dictionary of output column names and their dtype.
    kwargs: dict
        name-value of extra arguments.  These values are passed
        directly into the function.
    pessimistic_nulls : bool
        Whether or not apply_rows output should be null when any corresponding
        input is null. If False, all outputs will be non-null, but will be the
        result of applying func against the underlying column data, which
        may be garbage.


    Examples
    --------
    The user function should loop over the columns and set the output for
    each row. Loop execution order is arbitrary, so each iteration of
    the loop **MUST** be independent of each other.

    When ``func`` is invoked, the array args corresponding to the
    input/output are strided so as to improve GPU parallelism.
    The loop in the function resembles serial code, but executes
    concurrently in multiple threads.

    >>> import cudf
    >>> import numpy as np
    >>> df = cudf.DataFrame()
    >>> nelem = 3
    >>> df['in1'] = np.arange(nelem)
    >>> df['in2'] = np.arange(nelem)
    >>> df['in3'] = np.arange(nelem)

    Define input columns for the kernel

    >>> in1 = df['in1']
    >>> in2 = df['in2']
    >>> in3 = df['in3']
    >>> def kernel(in1, in2, in3, out1, out2, kwarg1, kwarg2):
    ...     for i, (x, y, z) in enumerate(zip(in1, in2, in3)):
    ...         out1[i] = kwarg2 * x - kwarg1 * y
    ...         out2[i] = y - kwarg1 * z

    Call ``.apply_rows`` with the name of the input columns, the name and
    dtype of the output columns, and, optionally, a dict of extra
    arguments.

    >>> df.apply_rows(kernel,
    ...               incols=['in1', 'in2', 'in3'],
    ...               outcols=dict(out1=np.float64, out2=np.float64),
    ...               kwargs=dict(kwarg1=3, kwarg2=4))
       in1  in2  in3 out1 out2
    0    0    0    0  0.0  0.0
    1    1    1    1  1.0 -2.0
    2    2    2    2  2.0 -4.0
'''
_doc_from_backend = '''Convert VDataFrame to VDataFrame'''
_doc_VDataFrame_to_csv = '''Convert CSV files to VDataFrame'''
_doc_VSeries_to_csv = '''Convert CSV files to VSeries'''
_doc_VDataFrame_to_pandas = '''Convert VDataFrame to Pandas DataFrame'''
_doc_VSeries_to_pandas = '''Convert VSeries to Pandas DataFrame'''
_doc_VDataFrame_to_numpy = '''Convert VDataFrame to Numpy array'''
_doc_VSeries_to_numpy = '''Convert VSeries to Numpy array'''
_doc_VDataFrame_compute = '''Fake compute(). Return self.'''
_doc_VDataFrame_map_partitions = '''Apply Python function on each DataFrame partition.

    Note that the index and divisions are assumed to remain unchanged.

    Parameters
    ----------
    func : function
        The function applied to each partition. If this function accepts
        the special ``partition_info`` keyword argument, it will recieve
        information on the partition's relative location within the
        dataframe.
    args, kwargs :
        Positional and keyword arguments to pass to the function.
        Positional arguments are computed on a per-partition basis, while
        keyword arguments are shared across all partitions. The partition
        itself will be the first positional argument, with all other
        arguments passed *after*. Arguments can be ``Scalar``, ``Delayed``,
        or regular Python objects. DataFrame-like args (both dask and
        pandas) will be repartitioned to align (if necessary) before
        applying the function; see ``align_dataframes`` to control this
        behavior.
    enforce_metadata : bool, default True
        Ignored
    transform_divisions : bool, default True
        Ignored
    align_dataframes : bool, default True
        Ignored
    meta : pd.DataFrame, pd.Series, dict, iterable, tuple, optional
        Ignored
'''
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
    import dask.dataframe
    import dask.distributed

    try:
        import dask_cudf
        import cudf  # See https://docs.rapids.ai/api/dask-cuda/nightly/install.html
    except ModuleNotFoundError:
        print("Please install cudf and dask_cudf via the rapidsai conda channel. "
              "See https://rapids.ai/start.html for instructions.")
        sys.exit(-1)
    BackEndDataFrame: Any = cudf.DataFrame
    BackEndSeries: Any = cudf.Series
    BackEnd = cudf

    _from_back: Any = dask_cudf.from_cudf

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_backend: Any = dask_cudf.from_cudf

    read_csv: Any = dask.dataframe.read_csv

    _VDataFrame: Any = dask_cudf.DataFrame
    _VSeries: Any = dask_cudf.Series

    pandas.DataFrame.to_pandas = lambda self: self
    pandas.DataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    dask.dataframe.DataFrame.to_pandas = lambda self: self.compute()
    dask.dataframe.DataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    dask.dataframe.Series.to_pandas = lambda self: self.compute()
    dask.dataframe.Series.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    dask.dataframe.DataFrame.to_backend = lambda self: self.compute()
    dask.dataframe.DataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    dask.dataframe.Series.to_backend = lambda self: self.compute()
    dask.dataframe.Series.to_backend.__doc__ = _doc_VDataFrame_to_pandas

    _VDataFrame.to_pandas = lambda self: self.compute().to_pandas()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

# %%
if VDF_MODE == Mode.dask:
    import pandas
    import numpy
    import dask
    import dask.dataframe

    BackEndDataFrame: Any = pandas.DataFrame
    BackEndSeries: Any = pandas.Series
    BackEnd = pandas

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series


    def _partition_apply_rows(
            self,
            fn,
            incols,
            outcols,
            kwargs,
    ):
        # The first invocation is with fake datas
        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype) for param, dtype in outcols.items()}
        fn(**params, **outputs, **kwargs)  # TODO: compiler la fn
        for col, data in outputs.items():
            self[col] = data
        return self


    def _apply_rows(self,
                    fn,
                    incols,
                    outcols,
                    kwargs,
                    pessimistic_nulls=True,  # FIXME: use it
                    cache_key=None,  # FIXME: use it
                    ):
        return self.map_partitions(_partition_apply_rows, fn, incols, outcols, kwargs)


    # TODO: apply_grouped. https://docs.rapids.ai/api/cudf/stable/user_guide/guide-to-udfs.html
    # TODO: apply_chunck.
    _from_back: Any = dask.dataframe.from_pandas

    delayed: Any = dask.delayed

    compute: Any = dask.compute

    concat: Any = dask.dataframe.multi.concat

    from_pandas: Any = dask.dataframe.from_pandas
    from_backend: Any = dask.dataframe.from_pandas

    read_csv: Any = dask.dataframe.read_csv

    BackEndDataFrame.to_pandas = lambda self: self
    BackEndDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    BackEndSeries.to_pandas = lambda self: self
    BackEndSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows

    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_backend = lambda self: self.compute()
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_backend = lambda self: self.compute()
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

# %%
if VDF_MODE == Mode.cudf:
    import cudf
    import pandas

    BackEndDataFrame: Any = cudf.DataFrame
    BackEndSeries: Any = cudf.Series
    BackEnd = cudf

    _VDataFrame: Any = cudf.DataFrame
    _VSeries: Any = cudf.Series


    def _from_back(
            data: Union[BackEndDataFrame, BackEndSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    def _read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return cudf.concat((cudf.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return cudf.read_csv(filepath_or_buffer, **kwargs)


    def _DataFrame_to_csv(self, filepath_or_buffer, **kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*", "")
        return self._old_to_csv(filepath_or_buffer, **kwargs)


    # noinspection PyUnusedLocal
    def compute(*args,
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> Tuple:
        return tuple(args)


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed

    concat: Any = cudf.concat

    read_csv: Any = _read_csv
    read_csv.__doc__ = cudf.read_csv.__doc__

    from_pandas: Any = _remove_dask_parameters(cudf.from_pandas)
    from_pandas.__doc__ = _doc_from_pandas
    from_backend: Any = _remove_dask_parameters(lambda self: self)
    from_backend.__doc__ = _doc_from_backend

    pandas.Series.to_pandas = lambda self: self
    pandas.Series.to_pandas.__doc__ = _doc_VDataFrame_to_pandas

    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    _VSeries.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__

    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _VDataFrame.to_pandas.__doc__
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _VSeries.to_pandas.__doc__

    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VDataFrame_compute

    if "_old_to_csv" not in _VDataFrame.__dict__:
        _VDataFrame._old_to_csv = _VDataFrame.to_csv
    _VDataFrame.to_csv = _remove_to_csv(_DataFrame_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv

    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize

    compute.__doc__ = _doc_compute

# %%
if VDF_MODE == Mode.pandas:
    import pandas
    import numpy

    # BackEndDataFrame = pandas.DataFrame
    # BackEndSeries = pandas.Series
    # _BackIndex = pandas.Index
    BackEndDataFrame: Any = pandas.DataFrame
    BackEndSeries: Any = pandas.Series
    BackEnd = pandas

    _VDataFrame: Any = pandas.DataFrame
    _VSeries: Any = pandas.Series


    # noinspection PyUnusedLocal
    def _from_back(  # noqa: F811
            data: Union[BackEndDataFrame, BackEndSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    def read_csv(filepath_or_buffer, **kwargs):
        if not isinstance(filepath_or_buffer, list):
            return pandas.concat((pandas.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return pandas.read_csv(filepath_or_buffer, **kwargs)


    # apply_rows is a special case of apply_chunks, which processes each of the DataFrame rows independently in parallel.
    def _apply_rows(
            self,
            func,
            incols,
            outcols,
            kwargs,
            pessimistic_nulls=True,  # FIXME: use it
            cache_key=None,  # FIXME: use it
    ):
        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype=dtype) for param, dtype in outcols.items()}
        func(**params, **outputs, **kwargs)  # TODO: compiler la fn
        for col, data in outputs.items():
            self[col] = data
        return self


    # noinspection PyUnusedLocal
    def compute(*args,  # noqa: F811
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> Tuple:
        return args


    def _DataFrame_to_csv(self, filepath_or_buffer, **kwargs):
        if "*" in str(filepath_or_buffer):
            filepath_or_buffer = filepath_or_buffer.replace("*", "")
        return self._old_to_csv(filepath_or_buffer, **kwargs)


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed

    concat: Any = pandas.concat

    compute.__doc__ = _doc_compute

    read_csv.__doc__ = pandas.read_csv.__doc__

    from_pandas: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_pandas.__doc__ = _doc_from_pandas
    from_backend: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_backend.__doc__ = _doc_from_backend

    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows

    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _VSeries.map.__doc__
    _VSeries.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__

    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VDataFrame_compute

    # Add fake to_pandas() in pandas
    _VDataFrame.to_pandas = lambda self: self
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_pandas = lambda self: self
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas

    if "_old_to_csv" not in _VDataFrame.__dict__:
        _VDataFrame._old_to_csv = _VDataFrame.to_csv
    _VDataFrame.to_csv = _remove_to_csv(_DataFrame_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv

    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize


# %%
class VDataFrame(_VDataFrame):
    ''' A *virtual* dataframe.
    The concret dataframe depend on the environment variable `VDF_MODE`.
    It's may be : `pandas.DataFrame`, `cudf.DataFrame` or `dask.DataFrame`
    '''

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
        return _from_back(
            BackEndDataFrame(data=data, index=index, columns=columns, dtype=dtype),
            npartitions=npartitions,
            chunksize=chunksize,
            sort=sort,
            name=name)


class VSeries(_VSeries):
    ''' A *virtual* series.
    The concret series depend on the environment variable `VDF_MODE`.
    It's may be : `pandas.Series`, `cudf.Series` or `dask.Series`
    '''

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
            BackEndSeries(data=data, index=index, dtype=dtype, name=name),
            npartitions=npartitions,
            chunksize=chunksize,
            sort=sort,
            name=name)


# %%
__all__: List[str] = ['VDF_MODE', 'Mode',
                      'VDataFrame', 'VSeries',
                      'delayed', 'compute',
                      ]
