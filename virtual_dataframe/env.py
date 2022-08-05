import logging
import os
from enum import Enum
from typing import List


LOGGER: logging.Logger = logging.getLogger(__name__)

_yes: List[str] = ["true", "y", "yes"]

# To use a synchronous scheduler, set DEBUG=y
DEBUG: bool = os.environ.get("DEBUG", "").lower() in _yes


# If GPU detected, set to True
# If GPU detected and USE_GPU=No, set to False,
# else set to False
class Mode(Enum):
    pandas = "pandas"
    cudf = "cudf"
    dask = "dask"
    dask_modin = "dask_modin"
    ray_modin = "ray_modin"
    dask_cudf = "dask_cudf"


USE_GPU = os.path.exists("/proc/driver/nvidia")
# try:
#     import GPUtil
#
#     USE_GPU: bool = os.environ.get("USE_GPU", "no").lower() in _yes if "USE_GPU" in os.environ \
#         else len(GPUtil.getAvailable()) > 0
# except ModuleNotFoundError:
#     USE_GPU = False

# Default is pandas
VDF_MODE: Mode = Mode[os.environ.get("VDF_MODE", "pandas").replace('-', '_')]

LOGGER.info(f"{DEBUG=} {VDF_MODE=}")
