from typing import Any

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
    def __init__(self, **kwargs):
        from pyspark.conf import SparkConf

        self.conf = SparkConf()
        self.conf.set("spark.master", "local[*]")  # Default value
        for k, v in kwargs.items():
            self.conf.set(k.replace("_", "."), v)
        self.session = None

    def __str__(self):
        return f"SparkLocalCluster(\'{self.conf.get('spark.master')}\')"

    def __repr__(self):
        return self.__str__()

    def __enter__(self):
        if not self.session:
            from pyspark.sql import SparkSession
            builder = SparkSession.builder
            for k, v in self.conf.getAll():
                builder.config(k, v)
            self.session = builder.getOrCreate()
            return self

    def __exit__(self, type: None, value: None, traceback: None) -> None:
        if self.session:
            self.session.__exit__(type, None, None)
            self.session = None


def _new_VLocalCluster(
        mode: Mode,
        **kwargs) -> Any:
    if mode in (Mode.pandas, Mode.cudf, Mode.modin):
        return _LocalClusterDummy()
    elif mode == Mode.pyspark:
        return SparkLocalCluster(**kwargs)
    elif mode in (Mode.dask, Mode.dask_modin):
        from dask.distributed import LocalCluster
        # Purge kwargs
        for key in params_cuda_local_cluster:
            if key in kwargs:
                del kwargs[key]
        return LocalCluster(**kwargs)
    elif mode == Mode.dask_cudf:
        try:
            from dask_cuda import LocalCUDACluster
            return LocalCUDACluster(**kwargs)
        except ModuleNotFoundError:
            raise ValueError(
                "Please install dask-cuda via the rapidsai conda channel. "
                "See https://rapids.ai/start.html for instructions.")


class VLocalCluster():
    def __new__(cls, **kwargs) -> Any:
        return _new_VLocalCluster(
            VDF_MODE,
            **kwargs)
