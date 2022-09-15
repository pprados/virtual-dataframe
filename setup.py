#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import subprocess

from setuptools import setup

PYTHON_VERSION = "3.8"

# USE_GPU="-gpu" ou "" si le PC possède une carte NVidia
# ou suivant la valeur de la variable d'environnement GPU (export GPU=yes)
def _check_gpu() -> bool:
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


USE_GPU: str = "-gpu" if _check_gpu() else ""

# Run package dependencies
requirements = [
    'python-dotenv>=0.20',
    'pandas>=1.3',
    'numba>=0.55',
    'numpy>=1.21',
    'GPUtil',
    ]

pandas_requirements = [
    'pandas>=1.4'
]

modin_requirements = [
#    'modin>=0.15'
    'modin'
]
dask_requirements = \
    [
        'dask>=2022.2', 'distributed>=2022.2',
    ]
#dask_modin_requirements = ['modin[dask]>=0.15']
dask_modin_requirements = ['modin[dask]']
#ray_modin_requirements = ['modin[ray]>=0.15']

all_requirements = set(pandas_requirements +
                                 dask_modin_requirements +
                                 # ray_modin_requirements +
                                 dask_requirements
                                 )

setup_requirements = ["pytest-runner", "setuptools_scm"]

# Packages for tests
test_requirements = [
    'pytest>=2.8',
    # 'pytest-openfiles', # For tests
    'pytest-xdist',
    # 'pytest-httpbin>=0.0.7',
    'pytest-mock',
    'pytest-cov>=3.0.0',

    'werkzeug==2.0',
    'papermill',
    'ipython',
]

# Package nécessaires aux builds mais pas au run
# FIXME Ajoutez les dépendances nécessaire au build et au tests à ajuster suivant le projet
dev_requirements = all_requirements.union([
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
    'jupyterlab-git',
    'mkdocs',
])


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
    description="Bridge between pandas, cudf, modin, dask and dask-cudf",
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
    python_requires=f'>={PYTHON_VERSION}',
    test_suite="tests",
    setup_requires=setup_requirements,
    # tests_require=test_requirements,
    extras_require={
        'dev': dev_requirements,
        'test': test_requirements,
        'pandas': pandas_requirements,
        'modin': modin_requirements,
        'dask': dask_requirements,
        'dask_modin': dask_modin_requirements,
        'all': all_requirements
    },
    # packages=find_packages(),
    packages=["virtual_dataframe"],
    package_data={"virtual_dataframe": ["py.typed"]},
    use_scm_version=True,  # Manage versions from Git tags
    install_requires=requirements,
)
