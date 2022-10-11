"""
A *Virtual Client* to facilitate the startup process.

With some environment variable, the program use different kind of scheduler.

Use 'VDF_CLUSTER' with protocol, host and optionaly, the port.
See README.md
"""
# @see https://blog.dask.org/2020/07/23/current-state-of-distributed-dask-clusters
import logging
import os
import sys
from typing import Any, Tuple, Dict, Optional, Union
from urllib.parse import urlparse, ParseResult

from .env import DEBUG, VDF_MODE, Mode, EnvDict
from .vlocalcluster import params_cuda_local_cluster

LOGGER: logging.Logger = logging.getLogger(__name__)
DASK_DEFAULT_PORT = 8787
RAY_DEFAULT_PORT = 10001

_global_client = None


def _analyse_cluster_url(mode: Mode, env) -> Tuple[ParseResult, Optional[str], int]:
    vdf_cluster = None
    # Domino server ?
    if "DASK_SCHEDULER_SERVICE_HOST" in env and \
            "DASK_SCHEDULER_SERVICE_PORT" in env:
        vdf_cluster = \
            f"{Mode.dask.name}://{env['DASK_SCHEDULER_SERVICE_HOST']}:{env['DASK_SCHEDULER_SERVICE_PORT']}"
    else:
        vdf_cluster = env.get("VDF_CLUSTER", None)
    if not vdf_cluster:
        if mode in (Mode.dask, Mode.dask_modin, Mode.dask_cudf):
            vdf_cluster = f"{Mode.dask.name}://threads"
        # elif mode == Mode.ray_modin:
        #     vdf_cluster = "ray://"
        else:
            vdf_cluster = ""
    parsed = urlparse(vdf_cluster)
    host = None
    port = -1
    if parsed.netloc:
        if ':' in parsed.netloc:
            host, port = parsed.netloc.split(':')
        else:
            host = parsed.netloc
        host = host.lower()
    if parsed.scheme == Mode.dask.name and port == -1:
        port = DASK_DEFAULT_PORT
    elif parsed.scheme == "ray" and port == -1:
        port = RAY_DEFAULT_PORT
    return parsed, host, int(port)


class _ClientDummy:
    def __init__(self, cluster):
        self.cluster = cluster

    def cancel(self, futures, asynchronous=None, force=False) -> None:
        pass

    def close(self, timeout='__no_default__') -> None:
        pass

    def __enter__(self) -> Any:
        return self

    def __exit__(self, type: None, value: None, traceback: None) -> None:
        pass

    def __str__(self) -> str:
        return "<Client: in-process scheduler>"

    def __repr__(self) -> str:
        return self.__str__()

    def shutdown(self) -> None:
        pass


_params_local_cluster = [
    "CUDA_VISIBLE_DEVICES",
    "n_workers",
    "threads_per_worker",
    "memory_limit",
    "device_memory_limit",
    "scheduler_port",
    "data",
    "local_directory",
    "shared_filesystem",
    "protocol",
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
    "worker_class",
    "pre_import",
]
_params_local_cuda_cluster = _params_local_cluster + [
    "name",
    "processes",
    "loop",
    "start",
    "host",
    "ip",
    "silence_logs",
    "dashboard_address",
    "worker_dashboard_address",
    "diagnostics_port",
    "services",
    "worker_services",
    "service_kwargs",
    "asynchronous",
    "security",
    "blocked_handlers",
    "interface",
    "scheduler_kwargs",
    "scheduler_sync_interval",
]


def _new_VClient(mode: Mode,
                 env: EnvDict,
                 **kwargs) -> Any:
    if mode in (Mode.pandas, Mode.cudf, Mode.modin):
        return _ClientDummy("")

    if "address" in kwargs and isinstance(getattr(kwargs["address"], "scheduler_address", None), str):
        assert mode in (Mode.dask, Mode.dask_cudf, Mode.dask_modin), "Compatible with Dask frameworks"
        import dask.distributed
        return dask.distributed.Client(**kwargs)
    else:
        vdf_cluster, host, port = _analyse_cluster_url(mode, env)

        if mode in (Mode.dask, Mode.dask_cudf, Mode.dask_modin):
            import dask
            assert vdf_cluster.scheme == Mode.dask.name
            if DEBUG:
                dask.config.set(scheduler='synchronous')  # type: ignore
                LOGGER.warning("Use synchronous scheduler for debuging")
            elif host in ('threads', '', None):
                if mode != Mode.dask_cudf:
                    dask.config.set(scheduler='threads')  # type: ignore
                    client = _ClientDummy("threads")
                else:
                    local_default_params = dask.config.global_config['local'] \
                        if 'local' in dask.config.global_config else {}
                    from dask_cuda import LocalCUDACluster
                    client = dask.distributed.Client(
                        address=LocalCUDACluster(**local_default_params),
                        **kwargs)
            elif host == 'processes':
                dask.config.set(scheduler='processes')  # type: ignore
                client = _ClientDummy("processes")
            else:
                if host.endswith(".local"):
                    local_default_params = dask.config.global_config['local'] \
                        if 'local' in dask.config.global_config else {}
                    if mode == Mode.dask_cudf:
                        from dask_cuda import LocalCUDACluster
                        client = dask.distributed.Client(address=
                                                         LocalCUDACluster(
                                                             **local_default_params
                                                         ),
                            **kwargs)
                    elif mode in (Mode.dask, Mode.dask_cudf):
                        # Purge params
                        for key in params_cuda_local_cluster:
                            if key in local_default_params:
                                del local_default_params[key]
                        client = dask.distributed.Client(
                            address=dask.distributed.LocalCluster(**local_default_params),
                            **kwargs)
                    else:
                        assert False, "Invalid VDF_MODE"
                else:
                    # Initialize for remote cluster
                    client = dask.distributed.Client(
                        address=f"{host}:{port}",
                        **kwargs)
                    LOGGER.warning(f"Use remote cluster on {host}:{port}")
        # elif mode == Mode.ray_modin:
        #     assert vdf_cluster.scheme == "ray"
        #     import ray
        #     ray_address = None
        #     if host:
        #         ray_address = f"ray://{host}:{port}" if host != "auto" else "auto"
        #
        #     if not ray_address:
        #         ray_context = ray.init()
        #     else:
        #         ray_context = ray.init(address=ray_address, **kwargs)
        #
        #     class RayClient:
        #         def __init__(self, ray_context):
        #             self.ray_context = ray_context
        #
        #         def cancel(self, futures, asynchronous=None, force=False) -> None:
        #             pass
        #
        #         def close(self, timeout='__no_default__') -> None:
        #             pass
        #
        #         def __enter__(self) -> Any:
        #             return self
        #
        #         def __exit__(self, type: None, value: None, traceback: None) -> None:
        #             # self.ray_context = None
        #             # ray.shutdown()
        #             pass
        #
        #         def __str__(self) -> str:
        #             return f"<Client: {self.ray_context.address_info['address']}>"
        #
        #         def __repr__(self) -> str:
        #             return self.__str__()
        #
        #     return RayClient(ray_context)
        else:
            assert False, "Invalid VDF_MODE"

    return client


class VClient():
    def __new__(cls, **kwargs) -> Any:
        # global _global_client
        # if not _global_client:
        #     _global_client = _new_VClient(
        #         VDF_MODE,
        #         os.environ,
        #         **kwargs)
        # return _global_client
        return _new_VClient(
            VDF_MODE,
            os.environ,
            **kwargs)
