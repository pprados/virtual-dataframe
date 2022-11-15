# Make commands

The Makefile contains the central entry points for common tasks related to this project.

## Commands
* `make help` will print all majors target
* `make configure`  will prepare the environment (conda venv, kernel, ...)
* `make lint` will lint the code
* `make test` will run all tests
* `make typing` will check the typing
* `make validate` will validate the version before commit
* `make clean` will clean current environment
* `make docs` will create and show a HTML documentation in 'build/'
* `make dist` will create a full wheel distribution

## Jupyter commands
* `make notebook` will start a jupyter notebook
* `make remove-kernel` will remove the project's kernel
* `make nb-run-%` will execute all notebooks
* `make nb-convert` will convert all notebooks to python
* `make clean-notebooks` will clean all datas in the notebooks

## Twine commands
* `make check-twine` will check the packaging before publication
* `make test-twine` will publish the package in `test.pypi.org <https://test.pypi.org>`_)
* `make twine` will publish the package in `pypi.org <https://pypi.org>`_)

## Conda commands
* `make conda-build` will build the conda package
* `make conda-debug` will build the package in debug mode
* `make conda-convert` will convert the package for all platform
* `make conda-install` will install the conda package
* `make conda-purge` will remove the build artefact
* `make conda-create` will create a test environment for each mode
* `make test-conda-forge` will test the conda-forge first step publication
* `make conda-forge` will publish the project to conda-forge


