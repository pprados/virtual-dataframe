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

# Mutual compatible versions
modin_ver = '0.13'
panda_ver = '1.4'
dask_ver = '2022.9'

# Run package dependencies
requirements = [
    'python-dotenv>=0.20',
    'GPUtil',
]

pandas_requirements = [
    f'pandas>={panda_ver}'
]

modin_requirements = [
    f'modin>={modin_ver}'
]
dask_requirements = \
    [
        f'dask>={dask_ver}',
        f'distributed>={dask_ver}',
    ]
dask_modin_requirements = [f'modin>={modin_ver}'] + dask_requirements
# ray_modin_requirements = ['modin[ray]>={modin_ver}']

# Pinned set of multualy compatible versions
all_requirements = [
    "pandas==1.4.*",
    "modin==0.13.*",
    "dask==2022.7.*",
    "distributed==2022.7.*",
]
setup_requirements = [
    #"pytest-runner",
    "setuptools_scm"
]

# Packages for tests
test_requirements = [
    'pytest>=2.8',
    'pytest-xdist',
    'pytest-mock',
    'pytest-cov>=3.0.0',

    'werkzeug==2.0',
    'papermill',
    'ipython',
]

# Package nécessaires aux builds mais pas au run
dev_requirements = set(all_requirements).union([
    'pip',
    'twine',  # To publish package in Pypi
    'flake8', 'pylint',  # For lint
    'daff',
    'pytype', 'mypy',
    'pandas-stubs',
    'mkdocs',

    'jupyterlab',
    'jupyterlab-git',
    'dask-labextension',
    'voila',

    # Extension
    'graphviz',
    'openpyxl',
    'sqlalchemy',
    'tables',
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
        'Development Status :: 4 - Beta',
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
