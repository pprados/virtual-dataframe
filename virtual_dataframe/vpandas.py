"""
Virtual Dataframe and Series.
"""
# flake8: noqa
import collections
import glob
import os
import sys
from functools import wraps, partial
from typing import Any, List, Tuple, Optional, Union, Callable, Dict, Type, Iterator
import warnings
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
_doc_VSeries_compute = '''Fake compute(). Return self.'''
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


# %%

def _remove_parameters(func, _params: List[str], *part_args, **kwargs):
    def wrapper(*args, **kwargs):
        for k in _params:
            kwargs.pop(k, None)
        return func(*args, **kwargs)

    return wrapper

def _not_implemented(*args,**kwargs):
    raise NotImplementedError()

_printed_warning = set()

def _warn(func, scope: str,
          ) -> Any:
    def _warn_fn(*args, **kwargs):
        if func not in _printed_warning:
            _printed_warning.add(func)
            warnings.warn(f"Function '{func.__name__}' not implemented in mode {scope}",
                          RuntimeWarning, stacklevel=0)
        return func(*args, **kwargs)
    return _warn_fn

def _remove_to_csv(func, *part_args, **kwargs):
    return _remove_parameters(func,
                              ["single_file",
                               "name_function",
                               "compute",
                               "scheduler",
                               "header_first_partition_only",
                               "compute_kwargs"])


# if VDF_MODE in (Mode.pandas, Mode.cudf, Mode.modin, Mode.dask_modin, Mode.ray_modin):
if VDF_MODE in (Mode.pandas, Mode.cudf, Mode.modin, Mode.dask_modin):

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
    import dask_cudf  # FIXME: a virer, c'est pour le typing local
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

    FrontEnd = dask.dataframe  # FIXME: dask_cudf ?

    _VDataFrame: Any = dask_cudf.DataFrame
    _VSeries: Any = dask_cudf.Series
    _from_back: Any = dask_cudf.from_cudf

    # High level functions
    delayed: Any = dask.delayed
    compute: Any = dask.compute
    visualize: Any = dask.visualize
    concat: _VDataFrame = dask.dataframe.multi.concat

    from_dict: _VDataFrame = FrontEnd.from_dict
    from_pandas: _VDataFrame = FrontEnd.from_pandas
    from_arrow: _VDataFrame = FrontEnd.frol_arrow  # !pandas, !dask
    from_backend: Any = dask_cudf.from_cudf

    read_csv = dask_cudf.read_csv  # dask_cudf
    read_excel = _not_implemented  # !dask_cudf
    read_fwf = FrontEnd.read_fwf  # dask_cudf
    read_hdf = FrontEnd.read_hdf  # dask_cudf
    read_json = dask_cudf.read_json  # dask_cudf
    read_orc = dask_cudf.read_orc  # dask_cudf
    read_parquet = dask_cudf.read_parquet  # dask_cudf
    read_sql = FrontEnd.read_sql  # dask_cudf
    read_sql_query = FrontEnd.read_sql_query  # dask_cudf
    read_sql_table = FrontEnd.read_sql_table  # dask_cudf
    read_table = FrontEnd.read_table  # dask_cudf

    # _VDataFrame.to_csv # dask_cudf
    # _VDataFrame.to_dict  # !dask_cudf
    # _VDataFrame.to_excel  # !dask_cudf
    # _VDataFrame.to_hdf # !dask_cudf
    # _VDataFrame.to_json # !dask_cudf
    # _VDataFrame.to_sql # !dask_cudf
    # _VDataFrame..to_orc # dask_cudf
    # _VDataFrame.to_parquet # dask_cudf

    # Add-on and patch of original dataframes and series
    pandas.DataFrame.to_pandas = lambda self: self
    pandas.DataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self.compute()
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_pandas = lambda self: self.compute().to_pandas()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy

    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VSeries.to_backend = lambda self: self.compute()
    _VSeries.to_backend.__doc__ = _doc_VDataFrame_to_pandas
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

    FrontEnd = dask.dataframe

    _VDataFrame: Any = dask.dataframe.DataFrame
    _VSeries: Any = dask.dataframe.Series
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


    # TODO: Implements CUDA specific method? (use cuda.threadIdx and cuda.blockDim)
    # apply_grouped. https://docs.rapids.ai/api/cudf/stable/user_guide/guide-to-udfs.html
    # apply_chunck? (https://docs.rapids.ai/api/cudf/nightly/api_docs/api/cudf.DataFrame.apply_chunks.html)

    # High level functions
    delayed: Any = dask.delayed
    compute: Any = dask.compute
    visualize: Any = dask.visualize
    concat: _VDataFrame = FrontEnd.multi.concat

    from_dict: _VDataFrame = FrontEnd.from_dict
    from_pandas: _VDataFrame = FrontEnd.from_pandas
    from_arrow: _VDataFrame = FrontEnd.from_arrow  # !pandas, !dask
    from_backend: _VDataFrame = FrontEnd.from_pandas

    read_csv = FrontEnd.read_csv  # dask
    read_excel = _not_implemented  # !dask
    read_fwf = FrontEnd.read_fwf  # dask
    read_hdf = FrontEnd.read_hdf  # dask
    read_json = FrontEnd.read_json  # dask
    read_orc = FrontEnd.read_orc  # dask
    read_parquet = FrontEnd.read_parquet  # dask
    read_sql = FrontEnd.read_sql  # dask
    read_sql_query = FrontEnd.read_sql_query  # dask
    read_sql_table = FrontEnd.read_sql_table  # dask
    read_table = FrontEnd.read_table  # dask

    # _VDataFrame.to_csv # dask
    # _VDataFrame.to_dict  # !dask
    # _VDataFrame.to_excel  # !dask
    # _VDataFrame.to_hdf # !dask
    # _VDataFrame.to_json # !dask
    # _VDataFrame.to_sql # !dask
    # _VDataFrame.to_orc # !dask
    # _VDataFrame.to_parquet # dask

    BackEndDataFrame.to_pandas = lambda self: self
    BackEndDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    BackEndSeries.to_pandas = lambda self: self
    BackEndSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas

    # Add-on and patch of original dataframes and series
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    pandas.DataFrame.to_pandas = lambda self: self
    pandas.DataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_pandas = lambda self: self.compute()
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self.compute()
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_numpy = lambda self: self.compute().to_numpy()
    _VDataFrame.to_numpy.__doc__ = _doc_VDataFrame_to_numpy

    _VSeries.to_pandas = lambda self: self.compute()
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_backend = lambda self: self.compute()
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_numpy = lambda self: self.compute().to_numpy()
    _VSeries.to_numpy.__doc__ = _doc_VSeries_to_numpy

    # TODO set_index parameters https://docs.dask.org/en/latest/generated/dask.dataframe.DataFrame.set_index.html#dask.dataframe.DataFrame.set_index
    # TODO client.persist(df)
    # TODO: df.persist()
    # TODO df.repartition
# %%
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


    def _to_csv(self, path_or_buf, **kwargs):
        if "*" in str(path_or_buf):
            path_or_buf = path_or_buf.replace("*", "")
        return self._old_to_csv(path_or_buf, **kwargs)


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

    concat: Any = cudf.concat

    from_dict: _VDataFrame = FrontEnd.from_dict
    from_pandas: _VDataFrame = _remove_dask_parameters(cudf.from_pandas)
    from_arrow: _VDataFrame = FrontEnd.from_arrow  # !pandas, !dask
    from_backend: _VDataFrame = _remove_dask_parameters(lambda self: self)

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    read_csv = _read_csv  # cudf
    read_excel = _not_implemented  # !cudf
    read_fwf = _not_implemented  # !cudf
    read_hdf = FrontEnd.read_hdf  # cudf
    read_json = FrontEnd.read_json  # cudf
    read_orc = FrontEnd.read_orc  # cudf
    read_parquet = FrontEnd.read_parquet  # cudf
    read_sql = _not_implemented  # !cudf
    read_sql_query = _not_implemented  # !cudf
    read_sql_table = _not_implemented  # !cudf
    read_table = _not_implemented  # !cudf

    read_csv.__doc__ = cudf.read_csv.__doc__

    _VDataFrame.to_csv = _remove_to_csv(_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv
    # _VDataFrame.to_dict: # !cudf
    # _VDataFrame.to_excel: # !cudf
    # _VDataFrame.to_excel: # !cudf
    # _VDataFrame.to_hdf  # !cudf
    # _VDataFrame.to_json  # !cudf
    # _VDataFrame.to_sql  # !cudf
    # _VDataFrame.to_orc  # !cudf
    # _VDataFrame.to_parquet  # !cudf

    pandas.Series.to_pandas = lambda self: self
    pandas.Series.to_pandas.__doc__ = _doc_VDataFrame_to_pandas

    # Add-on and patch of original dataframes and series
    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _doc_VDataFrame_map_partitions
    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _VDataFrame.to_pandas.__doc__
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VDataFrame.visualize = lambda self: visualize(self)
    _VDataFrame.visualize.__doc__ = _doc_VDataFrame_visualize

    _VSeries.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _VSeries.to_pandas.__doc__
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VSeries_compute
    _VSeries.visualize = lambda self: visualize(self)
    _VSeries.visualize.__doc__ = _doc_VSeries_visualize

    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize
    if "_old_to_csv" not in _VDataFrame.__dict__:
        _VDataFrame._old_to_csv = _VDataFrame.to_csv

# %%
# if VDF_MODE in (Mode.modin, Mode.dask_modin, Mode.ray_modin):
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

    import warnings

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


    def _read_csv(filepath_or_buffer, **kwargs) -> Union[_VDataFrame, Iterator[_VDataFrame]]:
        if not isinstance(filepath_or_buffer, list):
            return BackEnd.concat((BackEnd.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return BackEnd.read_csv(filepath_or_buffer, **kwargs)


    # apply_rows is a special case of apply_chunks, which processes each of the DataFrame rows independently in parallel.
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


    def _to_csv(self, path_or_buf, **kwargs):
        if "*" in str(path_or_buf):
            path_or_buf = path_or_buf.replace("*", "")
        return self._old_to_csv(path_or_buf, **kwargs)


    concat: Any = modin.pandas.concat

    from_dict: _VDataFrame = FrontEnd.from_dict  # !modin
    from_pandas: Any = lambda data, npartitions=1, chuncksize=None, sort=True, name=None: \
        modin.pandas.DataFrame(data) if isinstance(data, pandas.DataFrame) else modin.pandas.Series(data)
    from_arrow: _VDataFrame = FrontEnd.from_arrow  # !modin
    from_backend: Any = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    read_csv.__doc__ = modin.pandas.read_csv.__doc__

    read_csv = _read_csv  # modin
    read_excel = FrontEnd.read_excel  # modin
    read_fwf = FrontEnd.read_fwf  # modin
    read_hdf = FrontEnd.read_hdf  # modin
    read_json = FrontEnd.read_json  # modin
    read_orc = FrontEnd.read_orc  # modin
    read_parquet = FrontEnd.read_parquet  # modin
    read_sql = FrontEnd.read_sql  # modin
    read_sql_query = FrontEnd.read_sql_query  # modin
    read_sql_table = FrontEnd.read_sql_table  # modin
    read_table = FrontEnd.read_table  # modin

    _VDataFrame.to_csv = _to_csv  # modin
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv
    _VDataFrame.to_pandas = modin.pandas.DataFrame._to_pandas
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    # _VDataFrame.to_dict  # !modin
    # _VDataFrame.to_excel  # !modin
    # _VDataFrame.to_hdf  # !modin
    # _VDataFrame.to_json  # !modin
    # _VDataFrame.to_sql  # !modin
    # _VDataFrame.to_orc  # !modin
    # _VDataFrame.to_parquet  # !modin

    # TODO: to... series

    # Add-on and patch of original dataframes and series
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _VSeries.map.__doc__
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VDataFrame.visualize = lambda self: visualize(self)
    _VDataFrame.visualize.__doc__ = _doc_VDataFrame_visualize

    _VSeries.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VSeries_compute
    _VSeries.visualize = lambda self: visualize(self)
    _VSeries.visualize.__doc__ = _doc_VSeries_visualize
    _VSeries.to_pandas = modin.pandas.Series._to_pandas
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas
    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize

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


    def _read_csv(filepath_or_buffer, **kwargs) -> Union[_VDataFrame, Iterator[_VDataFrame]]:
        if not isinstance(filepath_or_buffer, list):
            return pandas.concat((pandas.read_csv(f, **kwargs) for f in glob.glob(filepath_or_buffer)))
        else:
            return pandas.read_csv(filepath_or_buffer, **kwargs)


    _read_csv.__doc__ = pandas.read_csv.__doc__

    # apply_rows is a special case of apply_chunks, which processes each of the DataFrame rows independently in parallel.
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


    def _to_csv(self, path_or_buf, **kwargs):
        if "*" in str(path_or_buf):
            path_or_buf = path_or_buf.replace("*", "")
        return self._old_to_csv(path_or_buf, **kwargs)


    delayed: Any = _delayed
    delayed.__doc__ = _doc_delayed
    concat: Any = pandas.concat

    # from_dict = FrontEnd.from_dict  # !pandas
    from_pandas = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df
    # from_arrow = FrontEnd.from_arrow  # !pandas, !dask
    from_backend = lambda df, npartitions=1, chuncksize=None, sort=True, name=None: df

    from_pandas.__doc__ = _doc_from_pandas
    from_backend.__doc__ = _doc_from_backend

    read_csv = FrontEnd.read_csv  # pandas
    read_excel = _warn(FrontEnd.read_excel, "cudf, dask or dask_cudf")  # pandas
    read_fwf = _warn(FrontEnd.read_fwf,"cudf")  # pandas
    read_hdf = FrontEnd.read_hdf  # pandas
    read_json = FrontEnd.read_json  # pandas
    read_orc = FrontEnd.read_orc  # pandas
    read_parquet = FrontEnd.read_parquet  # pandas
    read_sql = _warn(FrontEnd.read_sql,"cudf")  # pandas
    read_sql_query = _warn(FrontEnd.read_sql_query,"cudf")  # pandas
    read_sql_table = _warn(FrontEnd.read_sql_table,"cudf")  # pandas
    read_table = _warn(FrontEnd.read_table,"cudf")  # pandas

    # Add-on and patch of original dataframes and series
    _VDataFrame.apply_rows = _apply_rows
    _VDataFrame.apply_rows.__doc__ = _doc_apply_rows
    _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, *args, **kwargs)
    _VDataFrame.map_partitions.__doc__ = _VSeries.map.__doc__
    _VDataFrame.compute = lambda self, **kwargs: self
    _VDataFrame.compute.__doc__ = _doc_VDataFrame_compute
    _VDataFrame.visualize = lambda self: visualize(self)
    _VDataFrame.visualize.__doc__ = _doc_VDataFrame_visualize
    _VDataFrame.to_pandas = lambda self: self
    _VDataFrame.to_pandas.__doc__ = _doc_VDataFrame_to_pandas
    _VDataFrame.to_backend = lambda self: self
    _VDataFrame.to_backend.__doc__ = _doc_VDataFrame_to_pandas
    if "_old_to_csv" not in _VDataFrame.__dict__:
        _VDataFrame._old_to_csv = _VDataFrame.to_csv
    _VDataFrame.to_csv = _remove_to_csv(_to_csv)
    _VDataFrame.to_csv.__doc__ = _doc_VDataFrame_to_csv
    _VDataFrame.categorize = lambda self: self
    _VDataFrame.categorize.__doc__ = _doc_categorize

    _VSeries.map_partitions = lambda self, func, *args, **kwargs: self.map(func, *args, *kwargs)
    _VSeries.map_partitions.__doc__ = _VSeries.map.__doc__
    _VSeries.compute = lambda self, **kwargs: self
    _VSeries.compute.__doc__ = _doc_VSeries_compute
    _VSeries.visualize = lambda self: visualize(self)
    _VSeries.visualize.__doc__ = _doc_VSeries_visualize
    _VSeries.to_pandas = lambda self: self
    _VSeries.to_pandas.__doc__ = _doc_VSeries_to_pandas
    _VSeries.to_backend = lambda self: self
    _VSeries.to_backend.__doc__ = _doc_VSeries_to_pandas


# %%
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


# %%
__all__: List[str] = ['VDF_MODE', 'Mode',
                      'VDataFrame', 'VSeries',
                      'delayed', 'compute',
                      'BackEnd', 'BackEndDataFrame', 'BackEndSeries',
                      'FrontEnd'
                      ]
