from typing import Any

from . import vclient
from .env import VDF_MODE, Mode

params_cuda_local_cluster = [
    "CUDA_VISIBLE_DEVICES",
    "memory_limit",
    "device_memory_limit",
    "data",
    "local_directory",
    "shared_filesystem",
    "enable_tcp_over_ucx",
    "enable_infiniband",
    "enable_nvlink",
    "enable_rdmacm",
    "rmm_pool_size",
    "rmm_maximum_pool_size",
    "rmm_managed_memory",
    "rmm_async",
    "rmm_log_directory",
    "rmm_track_allocations",
    "jit_unspill",
    "log_spilling",
    "pre_import",
]


class _LocalClusterDummy:
    def __init__(self, **kwargs):
        self.scheduler_address = "localhost:8786"

    def __str__(self):
        return "LocalClusterDummy('localhost:8786')"

    def __repr__(self):
        return self.__str__()

    def __enter__(self):
        return self

    def __exit__(self, type: None, value: None, traceback: None) -> None:
        pass


class SparkLocalCluster:
    """
    This class provides a context manager
    for creating and managing a local Apache Spark cluster. The class is designed
    to be used as a context manager, so that the SparkSession object created by
    the class is automatically cleaned up at the end of the context.

    The constructor for the class takes any keyword arguments that should be
    passed to the Spark configuration. By default, the Spark master is
    set to `local[*]` which means that the cluster will run on the
    local machine using all available cores.
    """
    def __init__(self, **kwargs):

        conf = vclient.get_spark_conf()
        conf["spark.master"] = "local[*]"
        for k, v in kwargs.items():
            if k.startswith("spark_"):
                k = k.replace('_', '.')
                conf[k] = v
        self.spark_conf = conf
        self.session = None

    def __str__(self) -> str:
        """
        The __str__ and __repr__ methods are defined to return a string representation of the Spark cluster,
        based on the Spark master setting in the configuration.
        """
        return f"SparkLocalCluster(\'{self.spark_conf.get('spark.master')}\')"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        """
        The __enter__ method is called when entering the context and it creates a SparkSession object with the
        configuration settings. The method returns the instance of the SparkLocalCluster object.
        """
        if not self.session:
            from pyspark.sql import SparkSession
            builder = SparkSession.builder
            for k, v in self.spark_conf.items():
                builder.config(k, v)
            self.session = builder.getOrCreate()
            return self

    def __exit__(self, type: None, value: None, traceback: None) -> None:
        """
        The __exit__ method is called when the context is exited, and it cleans up the SparkSession
        object by calling its __exit__ method.
        """
        if self.session:
            self.session.__exit__(type, None, None)
            self.session = None


def _new_VLocalCluster(
        mode: Mode,
        **kwargs) -> Any:
    """
    This code defines a private function _new_VLocalCluster that returns an instance of a local
    cluster depending on the specified mode. The function takes any additional keyword arguments,
    which will be passed to the cluster constructor.

    If the mode is one of Mode.pandas, Mode.numpy, Mode.cudf, Mode.cupy, or Mode.modin,
    the function returns an instance of _LocalClusterDummy.

    If the mode is one of Mode.pyspark or Mode.pyspark_gpu, the function returns an
    instance of SparkLocalCluster, which is a wrapper around a pyspark.sql.SparkSession
    instance that runs in local mode.

    If the mode is one of Mode.dask, Mode.dask_array, or Mode.dask_modin, the function
    returns an instance of dask.distributed.LocalCluster, which is a cluster that runs in local mode.

    If the mode is one of Mode.dask_cudf or Mode.dask_cupy, the function tries to
    import dask_cuda.LocalCUDACluster and returns an instance of it if the import is successful.
    If the import fails, the function raises a ValueError asking the user to install dask-cuda.
    """
    if mode in (Mode.pandas, Mode.numpy, Mode.cudf, Mode.cupy, Mode.modin):
        return _LocalClusterDummy()
    elif mode in (Mode.pyspark, Mode.pyspark_gpu):
        return SparkLocalCluster(**kwargs)
    elif mode in (Mode.dask, Mode.dask_array, Mode.dask_modin):
        from dask.distributed import LocalCluster
        # Purge kwargs
        for key in params_cuda_local_cluster:
            if key in kwargs:
                del kwargs[key]
        return LocalCluster(**kwargs)
    elif mode in (Mode.dask_cudf, Mode.dask_cupy):
        try:
            from dask_cuda import LocalCUDACluster
            return LocalCUDACluster(**kwargs)
        except ModuleNotFoundError:
            raise ValueError(
                "Please install dask-cuda via the rapidsai conda channel. "
                "See https://rapids.ai/start.html for instructions.")


class VLocalCluster():
    """
    This code defines a class VLocalCluster with a __new__ method that returns the result of calling
    the _new_VLocalCluster function with VDF_MODE and the keyword arguments kwargs.
    VDF_MODE is a constant that specifies the mode of operation for some larger system,
    and _new_VLocalCluster is a function that returns a specific implementation of a local cluster based on the mode.
    So VLocalCluster is a convenient wrapper that creates the appropriate local cluster based on the specified mode.
    """
    def __new__(cls, **kwargs) -> Any:
        return _new_VLocalCluster(
            VDF_MODE,
            **kwargs)
