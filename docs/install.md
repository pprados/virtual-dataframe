## Installation

### Installing with Conda (recommended)
```shell
$ conda install -c conda-forge 'virtual_dataframe'
```

### Installing with pip
Use
```shell
$ pip install "virtual_dataframe"
```

### Installing from the GitHub master branch
```shell
$ pip install "virtual_dataframe@git+https://github.com/pprados/virtual-dataframe"
```

### Dependencies
You must install all others frameworks to use it with `virtual_dataframe`.

You can create a set of virtual environment, with the tools:
```shell
$ build-conda-vdf-env --help
```
like
```shell
$ build-conda-vdf-env pandas cudf dask_cudf
$ conda env list
$ conda activate vdf-cudf
$ conda activate vdf-dask_cudf-local
```

The `VDF_MODE` is set for each environment.
If you create an environment for a *dask* framework, two environment will be created.
One `vdf-XXX` where you must set the `VDF_CLUSTER` variable and another `vdf-XXX-local` with a pre set
of `VDF_CLUSTER=dask://.local` to use a *local* cluster.

You can find all environement Yaml file [here](https://github.com/pprados/virtual_dataframe/tree/develop/virtual_dataframe).

You can remove all versions with:
```shell
$ ./build-conda-vdf-envs.sh --remove
```
