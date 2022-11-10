import logging
import os
import sys
import warnings

from typing import List, Union, Dict

LOGGER: logging.Logger = logging.getLogger(__name__)

_yes: List[str] = ["true", "y", "yes"]

# To use a synchronous scheduler, set DEBUG=y
DEBUG: bool = os.environ.get("DEBUG", "").lower() in _yes

if sys.version_info.major == 3 and sys.version_info.minor >= 9:
    EnvDict = Union[Dict[str, str], os._Environ[str]]
else:
    EnvDict = Dict[str, str]

# If GPU detected, set to True
# If GPU detected and USE_GPU=No, set to False,
# else set to False
from enum import Enum


class Mode(Enum):
    pandas = "pandas"
    cudf = "cudf"
    dask = "dask"
    modin = "modin"
    pyspark = "pyspark"
    dask_modin = "dask_modin"
    # ray_modin = "ray_modin"
    dask_cudf = "dask_cudf"


def _check_cuda() -> bool:
    # if 'microsoft-standard' in os.uname().release:
    #     os.environ["LD_LIBRARY_PATH"] = f"/usr/lib/wsl/lib/:{os.environ.get('LD_LIBRARY_PATH','')}"
    # else:
    #     os.environ["LD_LIBRARY_PATH"] = f"/usr/local/cuda/compat:" \
    #                                     f"/usr/local/cuda/lib64:" \
    #                                     f"{os.environ.get('LD_LIBRARY_PATH','')}"
    # assert 'LD_LIBRARY_PATH' in os.environ, "LD_LIBRARY_PATH must be set Nvidia .so files"
    import GPUtil
    return len(GPUtil.getAvailable()) > 0


USE_GPU = _check_cuda()

# Default is pandas
_mode = os.environ.get("VDF_MODE", "pandas").replace('-', '_').strip()
if _mode not in Mode._value2member_map_:
    warnings.warn(f"Invalide VDF_MODE '{_mode}'. Use default 'pandas'",
                  stacklevel=0,
                  )
    _mode = "pandas"
VDF_MODE: Mode = Mode[_mode]

LOGGER.info(f"{DEBUG=} {VDF_MODE=}")
