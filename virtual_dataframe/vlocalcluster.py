import os
from typing import Any

from .env import DEBUG, VDF_MODE, Mode, EnvDict

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
        self.scheduler_address="localhost:8786"
    def __str__(self):
        return "LocalClusterDummy('localhost:8786')"
    def __repr__(self):
        return self.__str__()


def _new_VLocalCluster(
        mode: Mode,
        **kwargs) -> Any:
    if mode in (Mode.pandas, Mode.cudf, Mode.modin):
        return _LocalClusterDummy()
    elif mode in (Mode.dask, Mode.dask_modin):
        from distributed import LocalCluster
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
