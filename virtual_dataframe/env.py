import ctypes
import logging
import os

from typing import List

LOGGER: logging.Logger = logging.getLogger(__name__)

_yes: List[str] = ["true", "y", "yes"]

# To use a synchronous scheduler, set DEBUG=y
DEBUG: bool = os.environ.get("DEBUG", "").lower() in _yes

# If GPU detected, set to True
# If GPU detected and USE_GPU=No, set to False,
# else set to False
from enum import Enum


class Mode(Enum):
    pandas = "pandas"
    cudf = "cudf"
    dask = "dask"
    modin = "modin"
    dask_modin = "dask_modin"
    # ray_modin = "ray_modin"
    dask_cudf = "dask_cudf"


def _check_cuda() -> bool:
    if 'microsoft-standard' in os.uname().release:
        os.environ["LD_LIBRARY_PATH"] = f"/usr/lib/wsl/lib/:{os.environ['LD_LIBRARY_PATH']}"
    else:
        os.environ["LD_LIBRARY_PATH"] = f"/usr/local/cuda/compat:" \
                                        f"/usr/local/cuda/lib64:" \
                                        f"{os.environ['LD_LIBRARY_PATH']}"
    import GPUtil
    return len(GPUtil.getAvailable()) > 0


USE_GPU = _check_cuda()

# Default is pandas
VDF_MODE: Mode = Mode[os.environ.get("VDF_MODE", "pandas").replace('-', '_')]

LOGGER.info(f"{DEBUG=} {VDF_MODE=}")
