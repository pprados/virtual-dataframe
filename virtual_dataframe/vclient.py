"""
A *Virtual Client* to facilitate the startup process.
With some environment variable, the program use different kind of scheduler.
With some environment variable, the program use different kind of scheduler.

+-----------+-------+-----------------------------+------------------+
| VDF_MODE  | DEBUG | DASK_SCHEDULER_SERVICE_HOST | Scheduler        |
+===========+=======+=============================+==================+
| pandas    | -     | -                           | No scheduler     |
+-----------+-------+-----------------------------+------------------+
| cudf      | -     | -                           | No scheduler     |
+-----------+-------+-----------------------------+------------------+
| dask      | Yes   | -                           | synchronous      |
+-----------+-------+-----------------------------+------------------+
| dask      | No    | -                           | processes        |
+-----------+-------+-----------------------------+------------------+
| dask      | No    | localhost                   | LocalCluster     |
+-----------+-------+-----------------------------+------------------+
| dask-cudf | No    | localhost                   | LocalCUDACluster |
+-----------+-------+-----------------------------+------------------+
| dask-cudf | No    | <host>                      | Dask cluster     |
+-----------+-------+-----------------------------+------------------+


Sample:
``
from virtual_dataframe import VClient

with (VClient())
    # Now, use the scheduler
``
"""
# @see https://blog.dask.org/2020/07/23/current-state-of-distributed-dask-clusters
import logging
import os
import sys
from typing import Any

from .env import USE_CLUSTER, DEBUG, VDF_MODE, Mode

LOGGER: logging.Logger = logging.getLogger(__name__)


class _FakeClient():
    def cancel(self, futures, asynchronous=None, force=False) -> None:
        pass

    def close(self, timeout='__no_default__') -> None:
        pass

    def __enter__(self) -> None:
        pass

    def __exit__(self, type: None, value: None, traceback: None) -> None:
        pass

    def __str__(self) -> str:
        return "<Client: in-process scheduler>"

    def __repr__(self) -> str:
        return self.__str__()


class VClient():
    def __new__(cls, **kwargs) -> Any:
        client = _FakeClient()
        if VDF_MODE in (Mode.dask, Mode.dask_cudf):
            import dask.distributed
            if DEBUG:
                dask.config.set(scheduler='synchronous')  # type: ignore
                LOGGER.warning("Use synchronous scheduler for debuging")
            # Connect to Domino cluster
            elif "DASK_SCHEDULER_SERVICE_HOST" in os.environ and \
                    "DASK_SCHEDULER_SERVICE_PORT" in os.environ:

                host = os.environ["DASK_SCHEDULER_SERVICE_HOST"]
                port = os.environ["DASK_SCHEDULER_SERVICE_PORT"]
                if host.lower() in ("localhost", "127.0.0.1"):
                    if VDF_MODE == "dask_cudf":
                        from dask_cuda import LocalCUDACluster

                        client = dask.distributed.Client(LocalCUDACluster(), **kwargs)
                        LOGGER.warning("Use LocalCudaCluster scheduler")
                    else:
                        from distributed import LocalCluster

                        client = dask.distributed.Client(LocalCluster(), **kwargs)
                        LOGGER.warning("Use LocalCluster scheduler")

                # Initialize for remote cluster
                client = dask.distributed.Client(
                    address=f"{host}:{port}",
                    **kwargs)
                LOGGER.warning("Use remote cluster")
            elif USE_CLUSTER:

                if VDF_MODE == Mode.dask_cudf:  # Use in local or other environements

                    try:
                        from dask_cuda import LocalCUDACluster
                    except ModuleNotFoundError:
                        print(
                            "Please install dask-cuda via the rapidsai conda channel. "
                            "See https://rapids.ai/start.html for instructions.")
                        sys.exit(-1)

                    client = dask.distributed.Client(LocalCUDACluster(), **kwargs)
                    LOGGER.warning("Use LocalCudaCluster scheduler")
                else:
                    from distributed import LocalCluster

                    client = dask.distributed.Client(LocalCluster(), **kwargs)
                    LOGGER.warning("Use LocalCluster scheduler")
            else:
                # Use thread to schedule the job
                # This scheduler only provides parallelism when your computation
                # is dominated by non-Python code
                # See https://docs.dask.org/en/latest/scheduling.html#local-threads
                # dask.config.set(scheduler='threads')
                # "threads", "synchronous" or "processes"
                # dask.config.set(scheduler='processes')
                # client = dask.distributed.Client(processes=True)
                LOGGER.warning("Use processes scheduler")
        return client
