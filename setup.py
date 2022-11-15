#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import subprocess

from setuptools import setup, find_packages

PYTHON_VERSION = "3.8"

# Run package dependencies
requirements = [
    'pandas>1.4',
    'python-dotenv>=0.20',
    'GPUtil>=1.4.0',
]

setup_requirements = [
    # "pytest-runner",
    "pip",
    "setuptools_scm"
]

setup(
    name='virtual_dataframe',
    author="Philippe Prados",
    author_email="github@prados.fr",
    description="Bridge between pandas, cudf, modin, dask, dask-modin, dask-cudf, spark or spark+rapids",
    long_description=open('README.md', mode='r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/pprados/virtual_dataframe",
    license='Apache-2.0',
    keywords="dataframe",
    classifiers=[  # See https://pypi.org/classifiers/
        # Before release
        'Development Status :: 4 - Beta',
        # Development Status :: 5 - Production/Stable
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: ' + PYTHON_VERSION,
        'Operating System :: OS Independent',
        'Topic :: Scientific/Engineering',
    ],
    python_requires=f'>={PYTHON_VERSION}',
    include_package_data=True,
    test_suite="tests",
    setup_requires=setup_requirements,
    # tests_require=test_requirements,
    packages=[
        "virtual_dataframe",
        "virtual_dataframe.bin"
    ],
    package_data={
        "virtual_dataframe": ["py.typed"],
        "virtual_dataframe.bin": ["*"],
    },
    use_scm_version={
        'write_to': 'virtual_dataframe/_version.py',
    },
    install_requires=requirements,
    entry_points = {
        'console_scripts': [
            'build-conda-vdf-env = virtual_dataframe.bin.__init__:main'
        ]
    }
)
