"""
Virtual Dataframe and Series.
"""
# flake8: noqa
import glob
import os
import sys
import warnings
from functools import wraps

from pandas._typing import Axes, Dtype, IndexLabel
from typing import Any, List, Tuple, Optional, Union, Callable, Dict, Type, Iterator, Iterable, Sequence, Hashable

from .env import VDF_MODE, Mode

# %% docs
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
apply_rows(func, incols, outcols, kwargs, cache_key=None) method of cudf.core.dataframe.DataFrame instance
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
_doc_compute = '''Compute several collections at once.

    Parameters
    ----------
    args : object
        Any number of objects. If it is a @delayed object, it's computed and the
        result is returned. By default, python builtin collections are also
        traversed to look for @delayed.
    traverse : bool, optional
        ignore.
    scheduler : string, optional
        ignore.
    optimize_graph : bool, optional
        Ignore.
    get : ``None``
        Ignore.
    kwargs
        Ignore.
'''
_doc_visualize = '''Visualize several dask graphs simultaneously.

    Requires ``graphviz`` to be installed.
    Return 'empty image'
'''
_doc_from_backend = '''Convert VDataFrame to VDataFrame'''
_doc_VDataFrame_to_csv = '''Convert CSV files to VDataFrame'''
_doc_VSeries_to_csv = '''Convert CSV files to VSeries'''
_doc_VDataFrame_to_pandas = '''Convert VDataFrame to Pandas DataFrame'''
_doc_VSeries_to_pandas = '''Convert VSeries to Pandas DataFrame'''
_doc_VDataFrame_to_numpy = '''Convert VDataFrame to Numpy array'''
_doc_VSeries_to_numpy = '''Convert VSeries to Numpy array'''
_doc_VDataFrame_compute = '''Fake compute(). Return self.'''
_doc_VDataFrame_persist = '''Fake persist(). Return self.'''
_doc_VDataFrame_repartition = '''Fake repartition(). Return self.'''
_doc_VSeries_compute = '''Fake compute(). Return self.'''
_doc_VSeries_persist = '''Fake persist(). Return self.'''
_doc_VSeries_repartition = '''Fake repartition(). Return self.'''
_doc_VSeries_visualize = '''Fake visualize(). Return self.'''
_doc_VDataFrame_visualize = '''Fake visualize(). Return self.'''
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


# %% tools

def _remove_parameters(func, _params: List[str]):
    def wrapper(*args, **kwargs):
        for k in _params:
            kwargs.pop(k, None)
        return func(*args, **kwargs)

    return wrapper


def _not_implemented(*args, **kwargs):
    raise NotImplementedError()


_printed_warning = set()


def _warn(func: Callable, scope: str,
          ) -> Callable:
    def _wrapper(*args, **kwargs):
        if func.__name__ not in _printed_warning:
            _printed_warning.add(func.__name__)
            warnings.warn(f"Function '{func.__name__}' not implemented in mode {scope}",
                          RuntimeWarning, stacklevel=0)
        return func(*args, **kwargs)

    return _wrapper


def _rm_to(func):
    return _remove_parameters(func,
                              ["single_file",
                               "name_function",
                               "compute",
                               "scheduler",
                               "header_first_partition_only",
                               "compute_kwargs", ])


def _patch_to(func, rm_params, scope=None):
    def _patch(self, path_or_buf, *args, **kwargs):
        if scope and func not in _printed_warning:
            _printed_warning.add(func.__name__)
            warnings.warn(f"Function '{func.__name__}' not implemented in mode {scope}",
                          RuntimeWarning, stacklevel=0)
        if "*" in str(path_or_buf):
            path_or_buf = path_or_buf.replace("*", "")
        for k in rm_params:
            kwargs.pop(k, None)
        return func(self, path_or_buf, *args, **kwargs)

    _patch.__name__ = func.__name__
    return _patch


def _patch_read(front, func, scope=None):
    def _wrapper(filepath_or_buffer, *args, **kwargs) -> Union[_VDataFrame, Iterator[_VDataFrame]]:
        if scope:
            if func not in _printed_warning:
                _printed_warning.add(func)
                warnings.warn(f"Function '{func}' not implemented in mode {scope}",
                              RuntimeWarning, stacklevel=0)

        if not isinstance(filepath_or_buffer, list):
            return front.concat(
                [getattr(front, func)(f, *args[1:], **kwargs) for f in sorted(glob.glob(filepath_or_buffer))])
        return getattr(front, func)(filepath_or_buffer, *args[1:], **kwargs)

    _wrapper.__name__ = func
    return _wrapper


def _patch_pandas(pd_DataFrame, pd_Series):
    # Add-on and patch of original dataframes and series
    _extra_params = ["single_file",
                     "name_function",
                     "compute",
                     "scheduler",
                     "header_first_partition_only",
                     "compute_kwargs"]
    pd_DataFrame.to_csv = _patch_to(pd_DataFrame.to_csv, _extra_params)
    pd_DataFrame.to_excel = _patch_to(pd_DataFrame.to_excel, _extra_params, "cudf, dask and dask_cudf")
    pd_DataFrame.to_feather = _patch_to(pd_DataFrame.to_feather, _extra_params, "dask and dask_cudf")
    pd_DataFrame.to_fwf = _not_implemented
    pd_DataFrame.to_hdf = _patch_to(pd_DataFrame.to_hdf, _extra_params)
    pd_DataFrame.to_json = _patch_to(pd_DataFrame.to_json, _extra_params)
    pd_DataFrame.to_orc = _not_implemented
    pd_DataFrame.to_parquet = _patch_to(pd_DataFrame.to_parquet, _extra_params)

    pd_DataFrame.to_pandas = lambda self: self
    pd_DataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    pd_DataFrame.to_backend = lambda self: self
    pd_DataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas

    pd_DataFrame.apply_rows = _apply_rows
    pd_DataFrame.apply_rows.__doc__ = _doc_apply_rows
    pd_DataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    pd_DataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    pd_DataFrame.compute = lambda self, **kwargs: self
    pd_DataFrame.compute.__doc__ = _doc_VDataFrame_compute
    pd_DataFrame.persist = lambda self, **kwargs: self
    pd_DataFrame.persist.__doc__ = _doc_VDataFrame_persist
    pd_DataFrame.repartition = lambda self, **kwargs: self
    pd_DataFrame.repartition.__doc__ = _doc_VDataFrame_repartition
    pd_DataFrame.visualize = lambda self: visualize(self)
    pd_DataFrame.visualize.__doc__ = _doc_VDataFrame_visualize
    pd_DataFrame.categorize = lambda self: self
    pd_DataFrame.categorize.__doc__ = _doc_categorize

    pd_Series.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    pd_Series.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    pd_Series.compute = lambda self, **kwargs: self
    pd_Series.compute.__doc__ = _doc_VSeries_compute
    pd_Series.persist = lambda self, **kwargs: self
    pd_Series.persist.__doc__ = _doc_VSeries_persist
    pd_Series.repartition = lambda self, **kwargs: self
    pd_Series.repartition.__doc__ = _doc_VSeries_repartition
    pd_Series.visualize = lambda self: visualize(self)
    pd_Series.visualize.__doc__ = _doc_VSeries_visualize
    pd_Series.to_pandas = lambda self: self
    pd_Series.to_pandas.__doc__ = _doc_VSeries_to_pandas
    pd_Series.to_backend = lambda self: self
    pd_Series.to_backend.__doc__ = _doc_VSeries_to_pandas
    pd_Series.to_csv = _patch_to(pd_Series.to_csv, [], "cudf, dask and dask_cudf")
    pd_Series.to_excel = _patch_to(pd_Series.to_excel, [], "cudf, dask and dask_cudf")
    pd_Series.to_hdf = _patch_to(pd_Series.to_hdf, [], "dask_cudf")
    pd_Series.to_json = _patch_to(pd_Series.to_json, [], "dask_cudf")


def _patch_cudf(cu_DataFrame, cu_Series):
    _extra_params = ["single_file",
                     "name_function",
                     "compute",
                     "scheduler",
                     "header_first_partition_only",
                     "compute_kwargs"]

    cu_DataFrame.to_csv = _patch_to(cu_DataFrame.to_csv, _extra_params)
    cu_DataFrame.to_excel = _not_implemented
    cu_DataFrame.to_feather = _patch_to(cu_DataFrame.to_feather, _extra_params, "dask and dask_cudf")
    cu_DataFrame.to_fwf = _not_implemented
    cu_DataFrame.to_hdf = _patch_to(cu_DataFrame.to_hdf, _extra_params)
    cu_DataFrame.to_json = _patch_to(cu_DataFrame.to_json, _extra_params)
    cu_DataFrame.to_orc = _patch_to(cu_DataFrame.to_orc, _extra_params)
    _VDataFrameto_parquet = _patch_to(cu_DataFrame.to_parquet, _extra_params)
    cu_DataFrame.to_sql = _not_implemented

    pandas.Series.to_pandas = lambda self: self
    pandas.Series.to_pandas.__doc__ = _doc_VDataFrame_to_pandas

    # Add-on and patch of original dataframes and series
    cu_DataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    cu_DataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    cu_DataFrame.to_backend = lambda self: self
    cu_DataFrame.to_backend.__doc__ = cu_DataFrame.to_pandas.__doc__
    cu_DataFrame.compute = lambda self, **kwargs: self
    cu_DataFrame.compute.__doc__ = _doc_VDataFrame_compute
    cu_DataFrame.persist = lambda self, **kwargs: self
    cu_DataFrame.persist.__doc__ = _doc_VDataFrame_persist
    cu_DataFrame.repartition = lambda self, **kwargs: self
    cu_DataFrame.repartition.__doc__ = _doc_VDataFrame_repartition
    cu_DataFrame.visualize = lambda self: visualize(self)
    cu_DataFrame.visualize.__doc__ = _doc_VDataFrame_visualize
    cu_DataFrame.categorize = lambda self: self
    cu_DataFrame.categorize.__doc__ = _doc_categorize

    cu_Series.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    cu_Series.map_partitions.__doc__ = cu_Series.map.__doc__
    cu_Series.to_backend = lambda self: self
    cu_Series.to_backend.__doc__ = cu_Series.to_pandas.__doc__
    cu_Series.compute = lambda self, **kwargs: self
    cu_Series.compute.__doc__ = _doc_VSeries_compute
    cu_Series.persist = lambda self, **kwargs: self
    cu_Series.persist.__doc__ = _doc_VSeries_persist
    cu_Series.repartition = lambda self, **kwargs: self
    cu_Series.repartition.__doc__ = _doc_VSeries_repartition
    cu_Series.visualize = lambda self: visualize(self)
    cu_Series.visualize.__doc__ = _doc_VSeries_visualize
    cu_Series.to_csv = _not_implemented
    cu_Series.to_excel = _not_implemented
    cu_Series.to_hdf = _patch_to(cu_Series.to_hdf, [], "dask_cudf")
    cu_Series.to_json = _patch_to(cu_Series.to_json, [], "dask_cudf")


if VDF_MODE in (Mode.pandas, Mode.cudf, Mode.modin, Mode.dask_modin, Mode.pyspark):

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


    def _persist(*collections, traverse=True, optimize_graph=True, scheduler=None, **kwargs):
        return collections

# %% pandas
if VDF_MODE == Mode.pandas:
    import pandas
    import numpy

    # BackEndDataFrame = pandas.DataFrame
    # BackEndSeries = pandas.Series
    # _BackIndex = pandas.Index
    BackEndDataFrame: Any = pandas.DataFrame
    BackEndSeries: Any = pandas.Series
    BackEnd = pandas

    FrontEnd = pandas

    _VDataFrame: Any = FrontEnd.DataFrame
    _VSeries: Any = FrontEnd.Series


    # noinspection PyUnusedLocal
    def _from_back(  # noqa: F811
            data: Union[BackEndDataFrame, BackEndSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    # apply_rows is a special case of apply_chunks, which processes each of the DataFrame rows independently in parallel.
    _cache = {}  # type: ignore


    def _compile(func: Callable, cache_key: Optional[str]):
        import numba
        if cache_key is None:
            cache_key = func

        try:
            out = _cache[cache_key]
            return out
        except KeyError:
            kernel = numba.jit(func, nopython=True)
            _cache[cache_key] = kernel
            return kernel


    def _apply_rows(
            self,
            func: Callable,
            incols: Dict[str, str],
            outcols: Dict[str, Type],
            kwargs: Dict[str, Any],
            cache_key: Optional[str] = None,
    ):
        import numba

        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype=dtype) for param, dtype in outcols.items()}
        _compile(func, cache_key)(**params, **outputs, **kwargs)
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


    compute.__doc__ = _doc_compute


    def visualize(*args, **kwargs):
        try:
            import IPython
            return IPython.core.display.Image(data=[], url=None, filename=None, format=u'png', embed=None, width=None,
                                              height=None,
                                              retina=False)
        except ModuleNotFoundError:
            return True


    visualize.__doc__ = _doc_visualize

    delayed = _delayed
    delayed.__doc__ = _doc_delayed
    concat = pandas.concat
    persist = _persist  # type: ignore

    from_pandas = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    from_backend = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    # pandas
    read_csv = _patch_read(FrontEnd, "read_csv")
    read_excel = _patch_read(FrontEnd, "read_excel", "cudf, dask or dask_cudf")
    read_feather = _patch_read(FrontEnd, "read_feather", "dask and dask_cudf")
    read_fwf = _patch_read(FrontEnd, "read_fwf", "cudf and dask_cudf")
    read_hdf = _patch_read(FrontEnd, "read_hdf", "dask_cudf")
    read_json = _patch_read(FrontEnd, "read_json")
    read_orc = FrontEnd.read_orc
    read_parquet = FrontEnd.read_parquet
    read_sql_table = _warn(FrontEnd.read_sql_table, "cudf and dask_cudf")

    _patch_pandas(_VDataFrame, _VSeries)

# %% cudf
if VDF_MODE == Mode.cudf:
    import cudf
    import pandas

    BackEndDataFrame: Any = cudf.DataFrame
    BackEndSeries: Any = cudf.Series
    BackEnd = cudf

    FrontEnd = cudf

    _VDataFrame: Any = BackEndDataFrame
    _VSeries: Any = BackEndSeries


    def _from_back(
            data: Union[BackEndDataFrame, BackEndSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    def _read_csv(filepath_or_buffer, **kwargs) -> Union[_VDataFrame, Iterator[_VDataFrame]]:
        if not isinstance(filepath_or_buffer, list):
            return cudf.concat((cudf.read_csv(f, **kwargs) for f in sorted(glob.glob(filepath_or_buffer))))
        else:
            return cudf.read_csv(filepath_or_buffer, **kwargs)


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed


    # noinspection PyUnusedLocal
    def compute(*args,
                traverse: bool = True,
                optimize_graph: bool = True,
                scheduler: bool = None,
                get=None,
                **kwargs
                ) -> Tuple:
        return tuple(args)


    compute.__doc__ = _doc_compute


    def visualize(*args, **kwargs):
        try:
            import IPython
            return IPython.core.display.Image(data=[], url=None, filename=None, format=u'png', embed=None, width=None,
                                              height=None,
                                              retina=False)
        except ImportError:
            return True
        except ModuleNotFoundError:
            return True


    visualize.__doc__ = _doc_visualize

    persist = _persist  # type: ignore

    concat: Any = cudf.concat

    from_pandas: _VDataFrame = _remove_dask_parameters(cudf.from_pandas)
    from_backend: _VDataFrame = _remove_dask_parameters(lambda self: self)

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    # cudf
    read_csv = _patch_read(FrontEnd, "read_csv")
    read_excel = _not_implemented
    read_feather = _patch_read(FrontEnd, "read_feather", "dask and dask_cudf")
    read_fwf = _not_implemented
    read_hdf = _patch_read(FrontEnd, "read_hdf", "dask_cudf")
    read_json = _patch_read(FrontEnd, "read_json")
    read_orc = FrontEnd.read_orc
    read_parquet = FrontEnd.read_parquet
    read_sql_table = _not_implemented

    _patch_cudf(_VDataFrame, _VSeries)

# %% modin dask_modin
if VDF_MODE in (Mode.modin, Mode.dask_modin):
    if VDF_MODE == Mode.dask_modin:
        os.environ["MODIN_ENGINE"] = "dask"
    # elif VDF_MODE == Mode.ray_modin:
    #     os.environ["MODIN_ENGINE"] = "ray"
    else:
        os.environ["MODIN_ENGINE"] = "python"  # For debug
    import modin.pandas
    import pandas
    import numpy

    warnings.filterwarnings('module', '.*Distributing.*This may take some time\\.', )

    BackEndDataFrame: Any = modin.pandas.DataFrame
    BackEndSeries: Any = modin.pandas.Series
    BackEnd = modin.pandas

    FrontEnd = modin.pandas

    _VDataFrame: Any = BackEndDataFrame
    _VSeries: Any = BackEndSeries


    # noinspection PyUnusedLocal
    def _from_back(  # noqa: F811
            data: Union[BackEndDataFrame, BackEndSeries],
            npartitions: Optional[int] = None,
            chunksize: Optional[int] = None,
            sort: bool = True,
            name: Optional[str] = None,
    ) -> _VDataFrame:
        return data


    # apply_rows is a special case of apply_chunks, which processes each of the DataFrame rows independently in parallel.
    _cache = {}  # type: ignore


    def _compile(func: Callable, cache_key: Optional[str]):
        import numba
        if cache_key is None:
            cache_key = func

        try:
            out = _cache[cache_key]
            return out
        except KeyError:
            kernel = numba.jit(func, nopython=True)
            _cache[cache_key] = kernel
            return kernel


    def _apply_rows(
            self,
            func: Callable,
            incols: Dict[str, str],
            outcols: Dict[str, Type],
            kwargs: Dict[str, Any],
            cache_key: Optional[str] = None,
    ):

        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype=dtype) for param, dtype in outcols.items()}
        _compile(func, cache_key)(**params, **outputs, **kwargs)
        for col, data in outputs.items():
            self[col] = data
        return self


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed


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


    def visualize(*args, **kwargs):
        try:
            import IPython
            return IPython.core.display.Image(data=[], url=None, filename=None, format=u'png', embed=None, width=None,
                                              height=None,
                                              retina=False)
        except ModuleNotFoundError:
            return True


    visualize.__doc__ = _doc_visualize

    concat: Any = modin.pandas.concat
    persist = _persist

    from_pandas: Any = lambda data, npartitions=1, chuncksize=None, sort=True, name=None: \
        modin.pandas.DataFrame(data) if isinstance(data, pandas.DataFrame) else modin.pandas.Series(data)
    from_backend: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    # modin
    read_csv = FrontEnd.read_csv
    read_excel = _patch_read(FrontEnd, "read_excel", "cudf, dask and dask_cudf")
    read_feather = _patch_read(FrontEnd, "read_feather", "dask and dask_cudf")
    read_fwf = _patch_read(FrontEnd, "read_fwf", "cudf and dask_cudf")
    read_hdf = _patch_read(FrontEnd, "read_hdf", "dask_cudf")
    read_json = FrontEnd.read_json
    read_orc = _patch_read(FrontEnd, "read_orc")
    read_parquet = FrontEnd.read_parquet
    read_sql_table = _warn(FrontEnd.read_sql_table, "cudf and dask_cudf")

    # Add-on and patch of original dataframes and series
    _VDataFrame.to_orc = _not_implemented

    _VDataFrame.to_excel = _patch_to(_VDataFrame.to_excel, [], "cudf, dask and dask_cudf")
    _VDataFrame.to_feather = _patch_to(_VDataFrame.to_feather, [], "dask and dask_cudf")
    _VDataFrame.to_hdf = _patch_to(_VDataFrame.to_hdf, [], "dask_cudf")
    _VDataFrame.to_sql = _warn(_VDataFrame.to_sql, "cudf and dask_cudf")

    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas

    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    _VDataFrame.map_partitions = lambda df, func, *args, **kwargs: func(df, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VDataFrame.persist = lambda self, **kwargs: self
    _VDataFrame.persist.__doc__ = _doc_VDataFrame_persist
    _VDataFrame.repartition = lambda self, **kwargs: self
    _VDataFrame.repartition.__doc__ = _doc_VDataFrame_persist
    _VDataFrame.visualize = lambda self: visualize(self)
    _VDataFrame.visualize.__doc__ = _doc_VDataFrame_visualize
    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize
    _VDataFrame.to_pandas = _VDataFrame._to_pandas

    _VSeries.to_csv = _patch_to(_VSeries.to_csv, [], "cudf, dask and dask_cudf")
    _VSeries.to_excel = _patch_to(_VSeries.to_excel, [], "cudf, dask and dask_cudf")
    _VSeries.to_hdf = _patch_to(_VSeries.to_hdf, [], "dask_cudf")
    _VSeries.to_json = _patch_to(_VSeries.to_json, [], "dask_cudf")
    _VSeries.map_partitions = lambda df, func, *args, **kwargs: func(df, *args, **kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VSeries_compute
    _VSeries.persist = lambda self, **kwargs: self
    _VSeries.persist.__doc__ = _doc_VSeries_persist
    _VSeries.repartition = lambda self, **kwargs: self
    _VSeries.repartition.__doc__ = _doc_VSeries_repartition
    _VSeries.visualize = lambda self: visualize(self)
    _VSeries.visualize.__doc__ = _doc_VSeries_visualize
    _VSeries.to_pandas = modin.pandas.Series._to_pandas
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas

# %% dask_cudf
if VDF_MODE == Mode.dask_cudf:
    import pandas
    import dask
    import dask.dataframe

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

    FrontEnd = dask_cudf

    _VDataFrame: Any = dask_cudf.DataFrame
    _VSeries: Any = dask_cudf.Series
    _from_back: Any = dask_cudf.from_cudf

    # High level functions
    compute: Any = dask.compute
    concat: _VDataFrame = dask.dataframe.multi.concat
    delayed: Any = dask.delayed
    persist: Iterable[_VDataFrame] = dask.persist
    visualize: Any = dask.visualize


    def _from_pandas(df, npartitions=1):
        return dask_cudf.from_cudf(cudf.from_pandas(df), npartitions=npartitions)


    from_pandas = _from_pandas
    from_backend = dask_cudf.from_cudf


    def _patch_read_json(path_or_buf, **kwargs):
        if os.path.isdir(path_or_buf):
            path_or_buf = path_or_buf + "/*"
        return FrontEnd.read_json(path_or_buf, **kwargs)


    # dask_cudf
    read_csv = FrontEnd.read_csv
    read_excel = _not_implemented
    read_feather = _not_implemented
    read_fwf = _not_implemented
    read_hdf = _not_implemented
    read_json = _patch_read_json
    read_orc = FrontEnd.read_orc
    read_parquet = FrontEnd.read_parquet
    read_sql_table = _not_implemented

    _VDataFrame.to_excel = _not_implemented
    _VDataFrame.to_feather = _not_implemented
    _VDataFrame.to_fwf = _not_implemented
    _VDataFrame.to_hdf = _not_implemented
    _VDataFrame.to_sql = _not_implemented

    # Add-on and patch of original dataframes and series
    _VDataFrame.to_pandas = lambda self: self.compute().to_pandas()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self.compute()
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_pandas = lambda self: self.compute().to_pandas()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy

    _VSeries.to_pandas = lambda self: self.compute().to_pandas()
    _VSeries.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_backend = lambda self: self.compute()
    _VSeries.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

    _patch_cudf(BackEndDataFrame, BackEndSeries)

# %% dask
if VDF_MODE == Mode.dask:
    import pandas
    import numpy
    import dask
    import dask.dataframe

    BackEndDataFrame: Any = pandas.DataFrame
    BackEndSeries: Any = pandas.Series
    BackEnd = pandas

    FrontEnd = dask.dataframe

    _VDataFrame: Any = FrontEnd.DataFrame
    _VSeries: Any = FrontEnd.Series
    _from_back: Any = FrontEnd.from_pandas
    _cache = dict()  # type: Dict[Any, Any]


    def _compile(func: Callable, cache_key: Optional[str]):
        import numba

        if cache_key is None:
            cache_key = func

        try:
            out = _cache[cache_key]
            return out
        except KeyError:
            kernel = numba.jit(func, nopython=True)
            _cache[cache_key] = kernel
            return kernel


    def _partition_apply_rows(
            self,
            func: Callable,
            incols: Dict[str, str],
            outcols: Dict[str, Type],
            kwargs: Dict[str, Any],
    ):
        # The first invocation is with fake datas
        import numba
        kwargs = kwargs.copy()
        cache_key = kwargs["cache_key"]
        del kwargs["cache_key"]
        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype) for param, dtype in outcols.items()}
        _compile(func, cache_key)(**params, **outputs,
                                  **kwargs,
                                  )
        for col, data in outputs.items():
            self[col] = data
        return self


    def _apply_rows(self,
                    fn: Callable,
                    incols: Dict[str, str],
                    outcols: Dict[str, Type],
                    kwargs: Dict[str, Any],
                    cache_key: Optional[str] = None,
                    ):
        return self.map_partitions(_partition_apply_rows, fn, incols, outcols,
                                   # kwargs,
                                   {
                                       **kwargs,
                                       **{"cache_key": cache_key}
                                   }
                                   )


    # High level functions
    compute: Any = dask.compute
    concat: _VDataFrame = FrontEnd.multi.concat
    delayed: Any = dask.delayed
    persist: Iterable[_VDataFrame] = dask.persist
    visualize: Any = dask.visualize

    from_pandas: _VDataFrame = FrontEnd.from_pandas
    from_backend: _VDataFrame = FrontEnd.from_pandas


    def _patch_read_json(path_or_buf, **kwargs):
        if os.path.isdir(path_or_buf):
            path_or_buf = path_or_buf + "/*"
        return FrontEnd.read_json(path_or_buf, **kwargs)


    # dask
    read_csv = FrontEnd.read_csv
    read_excel = _not_implemented
    read_feather = _not_implemented
    read_fwf = _warn(FrontEnd.read_fwf, "cudf and dask_cudf")
    read_hdf = _warn(FrontEnd.read_hdf, "dask_cudf")
    read_json = _patch_read_json
    read_orc = FrontEnd.read_orc
    read_parquet = FrontEnd.read_parquet
    read_sql_table = _warn(FrontEnd.read_sql_table, "dask_cudf")

    _VDataFrame.to_excel = _not_implemented
    _VDataFrame.to_feather = _not_implemented
    _VDataFrame.to_fwf = _not_implemented

    # Add-on and patch of original dataframes and series
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self.compute()
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy


    def _patch_to_sql(f):
        def _to_sql(self, *p, **kwargs):
            if "con" in kwargs:
                kwargs["uri"] = kwargs.pop("con", None)
            return f(self, *p, **kwargs)

        return _to_sql


    _VDataFrame.to_sql = _patch_to_sql(_VDataFrame.to_sql)
    _VSeries.to_sql = _patch_to_sql(_VDataFrame.to_sql)

    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_backend = lambda self: self.compute()
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

    _patch_pandas(BackEndDataFrame, BackEndSeries)

# %% pyspark
if VDF_MODE == Mode.pyspark:

    import pandas
    from pandas._libs import lib
    import numpy
    import pyspark.pandas

    BackEndDataFrame: Any = pandas.DataFrame
    BackEndSeries: Any = pandas.Series
    BackEnd = pandas

    FrontEnd = pyspark.pandas

    _VDataFrame: Any = FrontEnd.DataFrame
    _VSeries: Any = FrontEnd.Series
    _from_back: Any = _remove_parameters(
        FrontEnd.from_pandas,
        [
            "npartitions",
            "chunksize",
            "sort",
            "name"
        ])
    _cache = dict()


    def _compile(func: Callable, cache_key: Optional[str]):
        import numba

        if cache_key is None:
            cache_key = func

        try:
            out = _cache[cache_key]
            return out
        except KeyError:
            kernel = numba.jit(func, nopython=True)
            _cache[cache_key] = kernel
            return kernel


    def _partition_apply_rows(
            self,
            func: Callable,
            incols: Dict[str, str],
            outcols: Dict[str, Type],
            kwargs: Dict[str, Any],
    ):
        # The first invocation is with fake datas
        import numba
        kwargs = kwargs.copy()
        cache_key = kwargs["cache_key"]
        del kwargs["cache_key"]
        size = len(self)
        params = {param: self[col].to_numpy() for col, param in incols.items()}
        outputs = {param: numpy.empty(size, dtype) for param, dtype in outcols.items()}
        _compile(func, cache_key)(**params, **outputs,
                                  **kwargs,
                                  )
        for col, data in outputs.items():
            if VDF_MODE == Mode.pyspark:
                self[col] = data.tolist()
            else:
                self[col] = data
        return self


    def _apply_rows(self,
                    fn: Callable,
                    incols: Dict[str, str],
                    outcols: Dict[str, Type],
                    kwargs: Dict[str, Any],
                    cache_key: Optional[str] = None,
                    ):
        return self.map_partitions(_partition_apply_rows, fn, incols, outcols,
                                   # kwargs,
                                   {
                                       **kwargs,
                                       **{"cache_key": cache_key}
                                   }
                                   )


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


    def visualize(*args, **kwargs):
        try:
            import IPython
            return IPython.core.display.Image(data=[], url=None, filename=None, format=u'png', embed=None, width=None,
                                              height=None,
                                              retina=False)
        except ModuleNotFoundError:
            return True


    visualize.__doc__ = _doc_visualize


    def _persist(*collections, traverse=True, optimize_graph=True, scheduler=None, **kwargs):
        return collections  # TODO: cache ?


    # High level functions
    concat = FrontEnd.concat
    delayed = _delayed
    persist = _persist  # type: ignore

    from_pandas: _VDataFrame = _remove_parameters(
        FrontEnd.from_pandas,
        [
            "npartitions",
            "chunksize",
            "sort",
            "name"
        ])
    from_backend: _VDataFrame = from_pandas


    def _patch_read_json(path_or_buf, **kwargs):
        if os.path.isdir(path_or_buf):
            path_or_buf = path_or_buf + "/*"
        return FrontEnd.read_json(path_or_buf, **kwargs)


    # pyspark
    read_csv = _patch_read(FrontEnd, "read_csv")
    # read_csv = FrontEnd.read_csv
    read_excel = _patch_read(FrontEnd, "read_excel", "cudf, dask, dask_modin and dask_cudf")
    read_feather = _not_implemented
    read_fwf = _not_implemented
    read_hdf = _not_implemented
    read_json = _patch_read(FrontEnd, "read_json")
    read_orc = _patch_read(FrontEnd, "read_orc")
    read_parquet = _patch_read(FrontEnd, "read_parquet")
    read_sql_table = _warn(FrontEnd.read_sql_table, "cudf and dask_cudf")

    _VDataFrame.to_feather = _not_implemented
    _VDataFrame.to_fwf = _not_implemented

    # Add-on and patch of original dataframes and series
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    _VDataFrame.to_backend = _VDataFrame.to_pandas
    _VDataFrame.to_backend.__doc__ = _VDataFrame.to_pandas.__doc__
    _VDataFrame.persist = lambda self, **kwargs: self
    _VDataFrame.persist.__doc__ = _doc_VSeries_persist
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VDataFrame.repartition = lambda self, **kwargs: self
    _VDataFrame.repartition.__doc__ = _doc_VDataFrame_repartition
    _VDataFrame.visualize = lambda self: visualize(self)
    _VDataFrame.visualize.__doc__ = _doc_VDataFrame_visualize
    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows

    _VDataFrame.to_sql = _not_implemented
    _VSeries.to_sql = _not_implemented

    _VSeries.to_backend = _VSeries.to_pandas
    _VSeries.to_backend.__doc__ = _VSeries.to_pandas.__doc__
    _VSeries.persist = lambda self, **kwargs: self
    _VSeries.persist.__doc__ = _doc_VSeries_persist
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VDataFrame_compute
    _VSeries.repartition = lambda self, **kwargs: self
    _VSeries.repartition.__doc__ = _doc_VSeries_repartition
    _VSeries.visualize = lambda self: visualize(self)
    _VSeries.visualize.__doc__ = _doc_VDataFrame_visualize
    _VSeries.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VSeries.map_partitions.__doc__ = _doc_VDataFrame_map_partitions

    _original_Series_to_excel = BackEnd.DataFrame.to_excel
    _original_Dataframe_to_excel = BackEnd.DataFrame.to_excel

    _patch_pandas(BackEndDataFrame, BackEndSeries)


    # Special case for pyspark

    def _series_to_excel(
            self,
            excel_writer,
            sheet_name: str = "Sheet1",
            na_rep: str = "",
            float_format: Optional[str] = None,
            columns: Optional[Union[str, List[str]]] = None,
            header: bool = True,
            index: bool = True,
            index_label: Optional[Union[str, List[str]]] = None,
            startrow: int = 0,
            startcol: int = 0,
            engine: Optional[str] = None,
            merge_cells: bool = True,
            encoding: Optional[str] = None,
            inf_rep: str = "inf",
            freeze_panes: Optional[Tuple[int, int]] = None,
    ) -> None:
        if "to_excel" not in _printed_warning:
            _printed_warning.add("to_excel")
            warnings.warn(f"Function 'to_excel' not implemented in mode cudf, dask and dask_cudf",
                          RuntimeWarning, stacklevel=0)
        _ = locals().copy()
        if isinstance(_["excel_writer"],str) and "*" in str(_["excel_writer"]):
            _["excel_writer"] = _["excel_writer"].replace("*", "")
        del _["self"]
        return _original_Dataframe_to_excel(self, **_)


    BackEnd.Series.to_excel = _series_to_excel


    def _dataframe_to_excel(
            self,
            excel_writer,
            sheet_name: str = "Sheet1",
            na_rep: str = "",
            float_format: Union[str, None] = None,
            columns: Union[Sequence[Hashable], None] = None,
            header: Union[Sequence[Hashable], bool] = True,
            index: bool = True,
            index_label: IndexLabel = None,
            startrow: int = 0,
            startcol: int = 0,
            engine: Union[str, None] = None,
            merge_cells: bool = True,
            encoding: lib.NoDefault = lib.no_default,
            inf_rep: str = "inf",
            verbose: lib.NoDefault = lib.no_default,
            freeze_panes: Union[Tuple[int, int], None] = None,
            # storage_options: StorageOptions = None,
    ) -> None:
        if "to_excel" not in _printed_warning:
            _printed_warning.add("to_excel")
            warnings.warn(f"Function 'to_excel' not implemented in mode cudf, dask and dask_cudf",
                          RuntimeWarning, stacklevel=0)
        _ = locals().copy()
        if isinstance(_["excel_writer"],str) and "*" in str(_["excel_writer"]):
            _["excel_writer"] = _["excel_writer"].replace("*", "")
        del _["self"]
        return _original_Dataframe_to_excel(self, **_)


    BackEnd.DataFrame.to_excel = _dataframe_to_excel


# %% Virtual
class VDataFrame(_VDataFrame):
    __test__ = False
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


# %% __all__
__all__: List[str] = ['VDF_MODE', 'Mode',
                      'VDataFrame', 'VSeries',
                      'delayed', 'compute', 'persist',
                      'BackEnd', 'BackEndDataFrame', 'BackEndSeries',
                      'FrontEnd',
                      'read_csv', 'read_excel', 'read_feather', 'read_fwf',
                      'read_hdf', 'read_json', 'read_orc',
                      'read_parquet',
                      'read_sql_table',
                      ]
