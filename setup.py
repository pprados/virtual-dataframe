#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import subprocess
from typing import List, Set

from setuptools import setup, find_packages

PYTHON_VERSION = "3.9"

# USE_GPU="-gpu" ou "" si le PC possède une carte NVidia
# ou suivant la valeur de la variable d'environnement GPU (export GPU=yes)
USE_GPU: str = "-gpu" if (os.environ['GPU'].lower() in 'yes'
                          if "GPU" in os.environ
                          else os.path.isdir("/proc/driver/nvidia")
                               or "CUDA_PATH" in os.environ) else ""

# Run package dependencies
requirements: List[str] = [
    'python-dotenv',
    'pandas',
    'numpy',
    'numba',
    # 'GPUtil',
    # With patch to accept cudf.
    'pandera@git+https://github.com/pprados/pandera@cudf#egg=pandera',
]

pandas_requirements: List[str] = \
    [
        'pandas'
    ]
cudf_requirements: List[str] = \
    [
        # 'cudf',               # Only with conda, see https://rapids.ai/start.html#conda
        # 'numba',
        # 'rapids==22.6',       # Only with conda, see https://rapids.ai/start.html#conda
        # 'cudatoolkit==11.2',  # Only with conda, and must be updated with the current cuda version
        # 'cudf==22.6',         # Only with conda
    ]
dask_requirements: List[str] = \
    [
        'dask[distributed]',  # 'dask[distributed]==2021.6.2',  # Version 2021.6.2 for DOMINO 4.6
    ]
dask_cudf_requirements: Set[str] = (
        cudf_requirements + dask_requirements + \
        [
            # 'dask-cudf',    # Only with conda
            # 'dask-cuda',    # Only with conda
        ])

all_requirements: Set[str] = set(pandas_requirements + cudf_requirements + dask_requirements + dask_cudf_requirements)

setup_requirements: List[str] = ["pytest-runner", "setuptools_scm"]

# Package nécessaires aux tests
test_requirements: List[str] = [
    'pytest>=2.8.0',
    # 'pytest-openfiles', # For tests
    'pytest-xdist',
    # 'pytest-httpbin>=0.0.7',
    'pytest-mock',

    'werkzeug==2.0.3',
    'papermill',
]

# Package nécessaires aux builds mais pas au run
# FIXME Ajoutez les dépendances nécessaire au build et au tests à ajuster suivant le projet
dev_requirements: List[str] = [
    'pip',
    'twine',  # To publish package in Pypi
    'sphinx', 'sphinx-execute-code', 'sphinx_rtd_theme', 'recommonmark', 'nbsphinx',  # To generate doc
    'flake8', 'pylint',  # For lint
    'daff',
    'pytype', 'mypy',
    'jupyterlab',
    'dask-labextension',
    'voila',
    'pandas-stubs',
    # For Dask
    'graphviz',
    'bokeh>=2.1.1',
]


# Return git remote url
def _git_url() -> str:
    try:
        with open(os.devnull, "wb") as devnull:
            out = subprocess.check_output(
                ["git", "remote", "get-url", "origin"],
                cwd=".",
                universal_newlines=True,
                stderr=devnull,
            )
        return out.strip()
    except subprocess.CalledProcessError:
        # git returned error, we are not in a git repo
        return ""
    except OSError:
        # git command not found, probably
        return ""


# Return Git remote in HTTP form
def _git_http_url() -> str:
    return re.sub(r".*@(.*):(.*).git", r"http://\1/\2", _git_url())


setup(
    name='virtual_dataframe',
    author="Philippe Prados",
    author_email="github@prados.fr",
    description="Bridge between pandas, cudf, dask and dask-cudf",
    long_description=open('README.md', mode='r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url=_git_http_url(),
    license='Private usage',
    keywords="dataframe",
    classifiers=[  # See https://pypi.org/classifiers/
        'Development Status :: 2 - PRE-ALPHA',
        # Before release
        # 'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Programming Language :: Python :: ' + PYTHON_VERSION,
        'Operating System :: OS Independent',
        # FIXME 'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
    python_requires='~=' + PYTHON_VERSION,
    test_suite="tests",
    setup_requires=setup_requirements,
    tests_require=test_requirements,
    extras_require={
        'dev': dev_requirements,
        'test': test_requirements,
        'pandas': pandas_requirements,
        'cudf': cudf_requirements,
        'dask': dask_requirements,
        'dask_cudf': dask_cudf_requirements,
        'dask-cudf': dask_cudf_requirements,
        'all': all_requirements
    },
    packages=find_packages(),
    package_data={"virtual_dataframe": ["py.typed"]},
    use_scm_version=True,  # Manage versions from Git tags
    install_requires=requirements,
)
