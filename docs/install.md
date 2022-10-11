## Installation

### Installing with Conda (recommended)
```shell
$ conda install -q -y \
	-c rapidsai -c nvidia -c conda-forge \
	'virtual_dataframe-all'
```
or, for only one mode:
```shell
$ VDF_MODE=...
$ conda install -q -y \
	-c rapidsai -c nvidia -c conda-forge \
	virtual_dataframe-$VDF_MODE
```
The package `virtual_dataframe` (without suffix) has no *framework* dependencies.
It's important to understand the implication of the dependencies (see below).

### Installing with pip
With PIP, it's not possible to install [NVidia Rapids](https://developer.nvidia.com/rapids) to use
the GPU. A limited list of dependencies is possible.

> :warning: **Warning: At this time, the packages are not published in pip or conda repositories**
Use
```shell
$ pip install "virtual_dataframe[all]@git+https://github.com/pprados/virtual-dataframe"
```
When the project were published, use
```shell
$ pip install "virtual_dataframe[all]"
```
or, for a selected framework
```shell
$ VDF_MODE=... # pandas, modin, dask or dask_modin only
$ pip install "virtual_dataframe[$VDF_MODE]"
```
The core of the framework can be installed with
```shell
$ pip install "virtual_dataframe"
```
It's important to understand the implication of the dependencies (see below).

### Installing from the GitHub master branch
```shell
$ pip install "virtual_dataframe[all]@git+https://github.com/pprados/virtual-dataframe"
```
or, for a selected framework
```shell
$ VDF_MODE=... # pandas, modin, dask or dask_modin only
$ pip install "virtual_dataframe[$VDF_MODE]@git+https://github.com/pprados/virtual-dataframe"
```
The core of the framework can be installed with
```shell
$ pip install "virtual_dataframe@git+https://github.com/pprados/virtual-dataframe"
```

### Dependencies
It's difficulte to use all the *pandas like* framework at the same time, in the same projet.
Each framework need some dependencies, differents of the others. But, with a specific selection
of versions of each framework, it's possible to merge all the frameworks.

We declare this dependencies with the defaults version of **virtual_dataframe**. Then, you can change only
the environment variable `VDF_MODE` to test the differents frameworks.

Else, it's possible to create a conda environement for each framework, with the last version of each one.
Then you must activate the specific environment and set the corresponding `VDF_MODE` variable.

> :warning: **Warning: At this time, the packages are not published in pip or conda repositories**

```shell
#!/usr/bin/env bash
# alias conda=mamba
VDF_MODES=pandas modin cudf dask dask_modin dask_cudf
for mode in $VDF_MODES
do
    conda create -y -n test-$mode \
      virtual_dataframe-$mode
    CONDA_PREFIX=$CONDA_HOME/envs/test-$mode \
    conda env config vars set VDF_MODE=$mode
done
```
Then, you can activate the specific environment to test your code.
```shell
$ conda activate test-cudf
```
