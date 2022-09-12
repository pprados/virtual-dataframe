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

from .env import DEBUG, VDF_MODE, Mode

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
            f"dask://{env['DASK_SCHEDULER_SERVICE_HOST']}:{env['DASK_SCHEDULER_SERVICE_PORT']}"
    else:
        vdf_cluster = env.get("VDF_CLUSTER", None)
    if not vdf_cluster:
        if mode in (Mode.dask, Mode.dask_modin, Mode.dask_cudf):
            vdf_cluster = "dask://localhost"
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
    if parsed.scheme == "dask" and port == -1:
        port = DASK_DEFAULT_PORT
    elif parsed.scheme == "ray" and port == -1:
        port = RAY_DEFAULT_PORT
    return parsed, host, int(port)


if sys.version_info.major == 3 and sys.version_info.minor >= 9:
    EnvDict = Union[Dict[str, str], os._Environ[str]]
else:
    EnvDict = Dict[str, str]


def _new_VClient(mode: Mode,
                 env: EnvDict,
                 **kwargs) -> Any:
    if mode in (Mode.pandas, Mode.cudf, Mode.modin):
        class _FakeClient:
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

        return _FakeClient()

    vdf_cluster, host, port = _analyse_cluster_url(mode, env)

    if mode in (Mode.dask, Mode.dask_cudf, Mode.dask_modin):
        import dask.distributed

        assert vdf_cluster.scheme == "dask"
        # import dask.distributed
        if DEBUG:
            dask.config.set(scheduler='synchronous')  # type: ignore
            LOGGER.warning("Use synchronous scheduler for debuging")
        else:
            if host in ("localhost", "127.0.0.1"):
                if mode == Mode.dask_cudf:
                    try:
                        from dask_cuda import LocalCUDACluster
                    except ModuleNotFoundError:
                        raise ValueError(
                            "Please install dask-cuda via the rapidsai conda channel. "
                            "See https://rapids.ai/start.html for instructions.")

                    client = dask.distributed.Client(LocalCUDACluster(), **kwargs)
                    LOGGER.warning("Use LocalCudaCluster scheduler")
                else:
                    from distributed import LocalCluster

                    client = dask.distributed.Client(LocalCluster(), **kwargs)
                    LOGGER.warning("Use LocalCluster scheduler")
            else:
                # Initialize for remote cluster
                client = dask.distributed.Client(
                    address=f"{host}:{port}",
                    **kwargs)
                LOGGER.warning(f"Use remote cluster on {host}:{port}")
            return client
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
        assert ("Invalid VDF_MODE")


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
