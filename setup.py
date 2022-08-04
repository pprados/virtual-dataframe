#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import re
import subprocess
import sys
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
    'python-dotenv==0.20.0',
    'pandas==1.4.3', # FIXME
    'numba',
]
dask_requirements: List[str] = \
    [
        'dask[distributed]',  # 'dask[distributed]==2021.6.2',  # Version 2021.6.2 for DOMINO 4.6
    ]

setup_requirements: List[str] = [
    "pytest-runner",
    "setuptools_scm",
]

# Package nécessaires aux tests
test_requirements: List[str] = [
    'pytest>=2.8.0',
    'pytest-mock',
    'pytest-xdist',
    # 'pytest-openfiles',
    # 'pytest-httpbin>=0.0.7',

    'werkzeug==2.0.3',
    'papermill',
]

# Package nécessaires aux builds mais pas au run
dev_requirements: List[str] = [
    'pip',
    'twine',  # To publish package in Pypi
    'conda-build',  # To publish package in conda
    'sphinx', 'sphinx-execute-code', 'sphinx_rtd_theme', 'recommonmark', 'nbsphinx',  # To generate doc
    'flake8', 'pylint',  # For lint
    'daff',
    'pytype', 'mypy', 'pandas-stubs',  # For test typing
    'jupyterlab',
    'voila',
    # For Dask
    'dask-labextension',
    'graphviz',
    'bokeh>=2.1.1',
    'dask-labextension',
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
    license='Apache v2',
    keywords="dataframe",
    classifiers=[  # See https://pypi.org/classifiers/
        'Development Status :: 2 - PRE-ALPHA',
        # Before release
        # 'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: ' + PYTHON_VERSION,
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering',
    ],
    python_requires='~=' + PYTHON_VERSION,
    test_suite="tests",
    setup_requires=setup_requirements,
    tests_require=test_requirements,
    extras_require={
        'dev': dev_requirements,
        'test': test_requirements,
        'dask': dask_requirements,
    },
    # packages=find_packages(where="virtual_dataframe"),
    #packages=find_packages(),
    packages=["virtual_dataframe"],
    package_data={"virtual_dataframe": ["py.typed"]},
    use_scm_version=True,  # Manage versions from Git tags
    install_requires=requirements,
)
