# Virtual DataFrame

## Motivation

This is a set of tools, to help the usage of Dask, with differents technology stack.

## Synopsis

With some parameters and Virtual classes, it's possible to write a code, and execute this code:
- With or without Dask
- With or without GPU
- With or without cluster

To do that, we create some virtual classe, add some method in others classes, etc.

It's difficult to use a combinaison of framework, with the same classe name, with simulare semantic, etc.
For exemple, if you want to use in the same program, Dask, cudf, pandas and panderas, you must manage:
- `pandas.DataFrame`, `pandas,Series`
- `cudf.DataFrame`, `cudf.Series`
- `dask.DataFrame`, `dask.Series`
- `panderas.DataFrame`, `panderas.Series`

 With `cudf`, the code must call `.to_pandas()`. With dask, the code must call `.compute()`, can use `@delayed` or
`dask.distributed.Client`. etc.

We propose to replace all these classes and scenarios, with a *uniform model*.
Then, it is possible to write one code, and use it in differents environnements and frameworks.

To reduce the confusion, you must use the classes `VDataFrame` and `VSeries` (The prefix `V` is for *Virtual*).
These classes propose the methods `.to_pandas()` and `.compute()` for each version.
And a new `@delayed` annotation can be use, with or without Dask.

With some parameter, the real classes may be `panda.DataFrame`, `cudf.DataFrame`, `dask.dataframe.DataFrame` with Pandas or
`dask.dataframe.DataFrame` with cudf (with Pandas or cudf for each partition).
And, it's possible to use Panderas for all `@delayed` methods to check the dataframe schema.

To manage the initialisation of a Dask cluster, you must use the `VClient()`. This alias, can be automatically initialized
with some environment variables.

```python
# Sample of code
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


    rc = my_function(VDataFrame({"data": [1, 2]})).compute()
    print(rc.to_pandas())

```

With this framework, you can update your environment, to debuging your code.

+--------------------------------------------------------------+---------------------------------------+
| env                                                          | Environement                          |
+--------------------------------------------------------------+---------------------------------------+
| VDF_MODE=pandas                                              | Only Python with classical pandas     |
| VDF_MODE=cudf                                                | Python with cuDF                      |
| VDF_MODE=pandas<br />DEBUG=True                              | Dask with single thread and pandas    |
| VDF_MODE=cudf<br />DEBUG=True                                | Dask with single thread and cuDF      |
| VDF_MODE=dask                                                | Dask with multiple process and pandas |
| VDF_MODE=dask-cudf                                           | Dask with multiple process and cuDF   |
| VDF_MODE=dask<br />DASK_SCHEDULER_SERVICE_HOST=locahost      | Dask with local cluster and pandas    |
| VDF_MODE=dask-cudf<br />DASK_SCHEDULER_SERVICE_HOST=locahost | Dask with local cuda cluster and cuDF |
| VDF_MODE=dask<br />DASK_SCHEDULER_SERVICE_HOST=...           | Dask with remote cluster and Pandas   |
| VDF_MODE=dask-cudf<br />DASK_SCHEDULER_SERVICE_HOST=...      | Dask with remote cluster and cuDF     |
+--------------------------------------------------------------+---------------------------------------+

The real compatibilty between the differents simulation of Pandas, depends on the implement of the cudf or dask.
You can use the `VDF_MODE` variable, to update some part of code, between the selected backend.

## The latest version

Clone the git repository (see upper button)

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
or to add type in code
```bash
$ make add-typing
```

To validate all the project
```bash
$ make validate
```

## Project Organization

    ├── Makefile                    <- Makefile with commands like `make data` or `make train`
    ├── README.md                   <- The top-level README for developers using this project.
    ├── data
    │   ├── external          <- Data from third party sources.
    │   ├── interim           <- Intermediate data that has been transformed.
    │   ├── processed         <- The final, canonical data sets for modeling.
    │   └── raw               <- The original, immutable data dump.
    │
    ├── docs                        <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── notebooks                   <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                                  the creator's initials, and a short `-` delimited description, e.g.
    │                                  `1.0-jqp-initial-data-exploration`.
    │
    ├── references                  <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports                     <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures           <- Generated graphics and figures to be used in reporting
    │
    ├── setup.py                    <- makes project pip installable (pip install -e .[tests])
    │                                  so sources can be imported and dependencies installed
    ├── virtual_dataframe           <- Source code for use in this project
    │   ├── __init__.py       <- Makes src a Python module
    │   ├── tools/__init__.py <- Python module to expose internal API
    │   └── tools/tools.py    <- Python module for functions, object, etc
    │
    └── tests                       <- Unit and integrations tests ((Mark directory as a sources root).


