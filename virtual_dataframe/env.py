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
    modin = "modin"
    dask_modin = "dask_modin"
    # ray_modin = "ray_modin"
    dask_cudf = "dask_cudf"


class _CHECK_CUDA(Enum):
    File = 0
    DLL = 1
    GPUtil = 2


_CHECK_CUDA_USE = _CHECK_CUDA.DLL  # FIXME: select strategy to detect cuda

if _CHECK_CUDA_USE == _CHECK_CUDA.File:
    def _check_cuda() -> bool:
        return os.path.exists("/proc/driver/nvidia")
elif _CHECK_CUDA_USE == _CHECK_CUDA.DLL:
    def _check_cuda() -> bool:
        import ctypes
        libnames = ('libcuda.so', 'libcuda.dylib', 'cuda.dll')
        for libname in libnames:
            try:
                cuda = ctypes.CDLL(libname)
                result = cuda.cuInit(0)
                if not result:
                    return True
            except OSError:
                continue
            else:
                break
        return False
# elif _CHECK_CUDA_USE == _CHECK_CUDA.GPUtil:
#     def _check_cuda() -> bool:
#         import GPUtil
#         return len(GPUtil.getAvailable()) > 0

USE_GPU = _check_cuda()

# Default is pandas
VDF_MODE: Mode = Mode[os.environ.get("VDF_MODE", "pandas").replace('-', '_')]

LOGGER.info(f"{DEBUG=} {VDF_MODE=}")
