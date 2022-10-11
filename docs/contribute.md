# Contribute

## Get the latest version

Clone the git repository

```bash
$ git clone --recurse-submodules https://github.com/pprados/virtual-dataframe.git
```

## Installation

Go inside the directory and
```bash
$ make configure
$ conda activate virtual_dataframe
$ make
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
    ├── docs                        <- A default mkdocs
    ├── conda_recipe                <- Script to build the conda package
    ├── notebooks                   <- Jupyter notebooks. Naming convention is a number (for ordering),
    ├── setup.py                    <- makes project pip installable (pip install -e .[tests])
    │                                  so sources can be imported and dependencies installed
    ├── virtual_dataframe           <- Source code for use in this project
    │   ├── __init__.py             <- Makes src a Python module
    │   └── *.py                    <- Framework codes
    │
    └── tests                       <- Unit and integrations tests ((Mark directory as a sources root).
