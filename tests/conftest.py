import math
import os
import sys
from typing import Dict
import pytest
import logging

import virtual_dataframe.numpy as vnp
import virtual_dataframe as vdf

_old_environ: Dict[str, str] = None


def _convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"

    neg = False
    if size_bytes < 0:
        neg = True
        size_bytes = -size_bytes

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    if neg:
        s = -s
    return f"{s} {size_name[i]}"

def save_context():
    global _old_environ
    _old_environ = dict(os.environ)


def restore_context():
    global _old_environ
    os.environ.clear()
    for k, v in _old_environ.items():
        os.environ[k] = v
    del sys.modules["virtual_dataframe.env"]
    del sys.modules["virtual_dataframe.vpandas"]



try:
    import nvidia_smi

    nvidia_smi.nvmlInit()

    def _dump_nvidia_memory() -> int:
        import logging

        handle = nvidia_smi.nvmlDeviceGetHandleByIndex(0)
        info = nvidia_smi.nvmlDeviceGetMemoryInfo(handle)
        logging.info(
            f"GPU {0}:"
            f"Memory : ({100 * info.free / info.total:.2f}% free) : "
            f"{_convert_size(info.total)} (total), "
            f"{_convert_size(info.free)} (free), "
            f"{_convert_size(info.used)} (used)")
        return info.free
except ImportError:
    def _dump_nvidia_memory() -> int:
        return 0

@pytest.fixture(scope="session")
def local_cluster():
    return vdf.VLocalCluster(
        scheduler_port=0,
        device_memory_limit="1g",
    )



@pytest.fixture(scope="session")
def vclient(local_cluster, request):
    client = vdf.VClient(
        address=local_cluster,
    )
    before = _dump_nvidia_memory()
    client.__enter__()

    def finalizer():
        client.__exit__(None, None, None)
        after = _dump_nvidia_memory()
        logging.info(f"GPU consume memory: {_convert_size(before - after)}")

    request.addfinalizer(finalizer)

    return client
