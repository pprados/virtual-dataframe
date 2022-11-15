import os
import sys
from typing import Dict

_old_environ: Dict[str, str] = None


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
