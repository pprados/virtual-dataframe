# Virtual DataFrame

## Motivation

This is a set of tools, to help the usage of Dask, with differents technology stack.

## Synopsis

With some parameters and Virtual classes, it's possible to write a code, and execute this code:
- With or without Dask
- With or without GPU
- With or without cluster

To do that, we create some virtual classes, add some methods in others classes, etc.

It's difficult to use a combinaison of framework, with the same classe name, with similare semantic, etc.
For example, if you want to use in the same program, Dask, cudf, pandas and panderas, you must manage:
- `pandas.DataFrame`, `pandas,Series`
- `cudf.DataFrame`, `cudf.Series`
- `dask.DataFrame`, `dask.Series`
- `panderas.DataFrame`, `panderas.Series`

 With `cudf`, the code must call `.to_pandas()`. With dask, the code must call `.compute()`, can use `@delayed` or
`dask.distributed.Client`. etc.

We propose to replace all these classes and scenarios, with a *uniform model*,
inspired by [dask](https://www.dask.org/).
Then, it is possible to write one code, and use it in differents environnements and frameworks.

This project is essentially a back-port of "Dask+Cudf" to others frameworks.

To reduce the confusion, you must use the classes `VDataFrame` and `VSeries` (The prefix `V` is for *Virtual*).
These classes propose the methods `.to_pandas()` and `.compute()` for each version.
And a new `@delayed` annotation can be use, with or without Dask.

With some parameters, the real classes may be `panda.DataFrame`, `cudf.DataFrame`, `dask.dataframe.DataFrame` with Pandas or
`dask.dataframe.DataFrame` with cudf (with Pandas or cudf for each partition).
And, it's possible to use [Panderas](https://pandera.readthedocs.io/en/stable/)
for all `@delayed` methods to check the dataframe schema.

To manage the initialisation of a Dask cluster, you must use the `VClient()`. This alias, can be automatically initialized
with some environment variables.

```python
# Sample of code, compatible Pandas, cudf, dask and dask-cudf
from virtual_dataframe import *
import pandera


class SimpleDF_schema(pandera.SchemaModel):
    id: pandera.typing.Index[int]
    data: pandera.typing.Series[int]

    class Config:
        strict = True
        ordered = True


TestDF = pandera.typing.DataFrame[SimpleDF_schema]

with (VClient()):
    @delayed
    @pandera.check_types
    def my_function(data: TestDF) -> TestDF:
        return data


    rc = my_function(VDataFrame({"data": [1, 2]}, npartitions=2)).compute()
    print(rc.to_pandas())

```

With this framework, you can update your environment, to debuging your code.

| env                                                                                            | Environement                          |
|------------------------------------------------------------------------------------------------|---------------------------------------|
| VDF_MODE=pandas                                                                                | Only Python with classical pandas     |
| VDF_MODE=cudf                                                                                  | Python with cuDF                      |
| VDF_MODE=dask                                                                                  | Dask with multiple process and pandas |
| VDF_MODE=dask-cudf                                                                             | Dask with multiple process and cuDF   |
| VDF_MODE=dask<br />DEBUG=True                                                                  | Dask with single thread and pandas    |
| VDF_MODE=dask-cudf<br />DEBUG=True                                                             | Dask with single thread and cuDF      |
| VDF_MODE=dask<br />DASK_SCHEDULER_SERVICE_HOST=locahost                                        | Dask with local cluster and pandas    |
| VDF_MODE=dask-cudf<br />DASK_SCHEDULER_SERVICE_HOST=locahost                                   | Dask with local cuda cluster and cuDF |
| VDF_MODE=dask<br />DASK_SCHEDULER_SERVICE_HOST=...<br />DASK_SCHEDULER_SERVICE_PORT=...        | Dask with remote cluster and Pandas   |
| VDF_MODE=dask-cudf<br />DASK_SCHEDULER_SERVICE_HOST=...<br />DASK_SCHEDULER_SERVICE_PORT=...   | Dask with remote cluster and cuDF     |

The real compatibilty between the differents simulation of Pandas, depends on the implement of the cudf or dask.
You can use the `VDF_MODE` variable, to update some part of code, between the selected backend.

It's not always easy to write a code *compatible* with all scenario, but it's possible.
Generally, add just `.compute()` and/or `.to_pandas()` is enough.
After this effort, it's possible to compare the performance about the differents technologies,
or propose a component, compatible with differents contexts.

## usage
```shell
$ conda install -q -y -c rapidsai -c nvidia -c conda-forge \
		dask-cuda \
		dask-cudf \
		cudf==22.06 \
		cudatoolkit==11.5
$ pip install -e virtual_dataframe
```

## API

| api                                    | comments                                       |
|----------------------------------------|------------------------------------------------|
| @delayed                               | Delayed function                               |
| vdf.concat(...)                        | Merge VDataFrame                               |
| vdf.read_csv(...)                      | Read VDataFrame from CSVs *glob* files         |
| vdf.from_pandas(pdf, npartitions=...)  | Create Virtual Dataframe from Pandas DataFrame |
| vdf.from_virtual(vdf, npartitions=...) | Create FIXME                                   |
| vdf.compute([...])                     | Compute multiple @delayed functions            |
| VDataFrame(data, npartitions=...)      | Create DataFrame in memory                     |
| VSeries(data, npartitions=...)         | Create Series in memory                        |
| VDataFrame.compute()                   | Compute the virtual dataframe                  |
| VDataFrame.to_pandas()                 | Convert to pandas dataframe                    |
| VDataFrame.to_csv()                    | Save to *glob* files                           |
| VDataFrame.to_numpy()                  | Convert to numpy array                         |
| VDataFrame.categorize()                | Detect all categories                          |
| VDataFrame.apply_rows()                | Apply rows, GPU template                       |
| VDataFrame.map_partitions()            | Apply function for each parttions              |
| VSeries.compute()                      | Compute the virtual series                     |
| VSeries.to_pandas()                    | Convert to pandas dataframe                    |
| VSeries.to_numpy()                     | Convert to numpy array                         |
| TODO                                   | ...                                            |

You can read a sample notebook [here](https://github.com/pprados/virtual-dataframe/blob/master/notebooks/demo.ipynb).

## Compatibility
This project is just a wrapper. So, it inherits limitations and bugs from other projects. Sorry for that.


| Limitations                                                                                     |
|-------------------------------------------------------------------------------------------------|
| <br />**pandas**                                                                                |
| All data must be in DRAM                                                                        |
| <br />**[cudf](https://docs.rapids.ai/api/cudf/nightly/user_guide/pandas-comparison.html)**     |
| All data must be in VRAM                                                                        |
| All data types in cuDF are nullable                                                             |
| Iterating over a cuDF Series, DataFrame or Index is not supported.                              |
| Join (or merge) and groupby operations in cuDF do not guarantee output ordering.                |
| The order of operations is not always deterministic                                             |
| Cudf does not support duplicate column names                                                    |
| Cudf also supports .apply() it relies on Numba to JIT compile the UDF and execute it on the GPU |
| .apply(result_type=...) not supported                                                           |
| <br />**[dask](https://distributed.dask.org/en/stable/limitations.html)**                       |
|  transpose() and MultiIndex are not implemented                                                 |
| Column assignment doesn't support type list                                                     |
| <br />**dask-cudf**                                                                             |
| See cudf and dask.                                                                              |
| Categories with strings not implemented                                                         |

To be compatible with all framework, you must only use the common features.

|     | small data          | big data                 |
|-----|---------------------|--------------------------|
| CPU | pandas<br/>Limite:+ | dask<br/>Limite:++       |
| GPU | cudf<br/>Limite:++  | dask_cudf<br/>Limite:+++ |

To develop, you can choose the level to be compatible with others frameworks.
Each cell is strongly compatible with the upper left part.

## No need of GPU?
If you don't need a GPU, then develop for `dask`.

|     | small data              | big data                 |
|-----|-------------------------|--------------------------|
| CPU | **pandas<br/>Limite:+** | **dask<br/>Limite:++**   |
| GPU | cudf<br/>Limite:++      | dask_cudf<br/>Limite:+++ |

You can ignore this API:
- `VDataFrame.apply_rows()`

## No need of big data?

If you don't need to use big data, then develop for `cudf`.

|     | small data              | big data                 |
|-----|-------------------------|--------------------------|
| CPU | **pandas<br/>Limite:+** | dask<br/>Limite:++       |
| GPU | **cudf<br/>Limite:++**  | dask_cudf<br/>Limite:+++ |

You can ignore these API:
- `@delayed`
- `map_partitions()`
- `categorize()`
- `compute()`
- `npartitions=...`

## Need all possibility?

To be compatible with all mode, develop for `dask_cudf`.

|     | small data              | big data                     |
|-----|-------------------------|------------------------------|
| CPU | **pandas<br/>Limite:+** | **dask<br/>Limite:++**       |
| GPU | **cudf<br/>Limite:++**  | **dask_cudf<br/>Limite:+++** |

and accept all the limitations.

## FAQ

### The code run with dask, but not with pandas or cudf ?
You must use only the similare functionality, and only a subpart of Pandas.
Develop for dask_cudf. it's easier to be compatible with others frameworks.

### `.compute()` is not defined with pandas, cudf ?
If you `@delayed` function return something, other than a `VDataFrame` or `VSerie`, the objet has not
the method `.compute()`. You can solve this, with:
```
@delayed
def f()-> int:
    return 42

real_result,=compute(f())  # Warning, compute return a tuple. The comma is important.
a,b = compute(f(),f())
```

## Pip install
Before using virtual_dataframe with GPU, use conda environment, and install some packages:
```shell
$ conda install -c rapidsai -c nvidia -c conda-forge cudf cudatoolkit dask-cuda dask-cudf
```

To install all feature of virtual_dataframe:
```shell
$ pip install virtual_dataframe[all]
```
later, we will package virtual_dataframe with conda dependencies.

To select only a part of dependencies, use pip with specific extension.
```shell

$ pip install virtual_dataframe[pandas]
$ pip install virtual_dataframe[cudf]
$ pip install virtual_dataframe[dask]
$ pip install virtual_dataframe[dask-cudf]
```
## The latest version

Clone the git repository

```bash
$ git clone --recurse-submodules https://github.com/pprados/virtual-dataframe.git
```

## Installation

Go inside the directory and
```bash
$ make configure
$ conda activate virtual_dataframe
$ make docs
```

## Tests

To test the project
```bash
$ make test
```

To validate the typing
```bash
$ make typing
```

To validate all the project
```bash
$ make validate
```

## Project Organization

    ├── Makefile                    <- Makefile with commands like `make data` or `make train`
    ├── README.md                   <- The top-level README for developers using this project.
    ├── data
    ├── docs                        <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── notebooks                   <- Jupyter notebooks. Naming convention is a number (for ordering),
    │   │                              the creator's initials, and a short `-` delimited description, e.g.
    │   │                              `1.0-jqp-initial-data-exploration`.
    │   └── demo.ipynb              <- A demo for all API
    │
    ├── reports                     <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures                 <- Generated graphics and figures to be used in reporting
    │
    ├── setup.py                    <- makes project pip installable (pip install -e .[tests])
    │                                  so sources can be imported and dependencies installed
    ├── virtual_dataframe           <- Source code for use in this project
    │   ├── __init__.py             <- Makes src a Python module
    │   ├── *.py                    <- Framework codes
    │   ├── tools/__init__.py       <- Python module to expose internal API
    │   └── tools/tools.py          <- Python module for functions, object, etc
    │
    └── tests                       <- Unit and integrations tests ((Mark directory as a sources root).


