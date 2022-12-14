# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.6] - 2022-12-XX TODO

Add support of Numpy frameworks (numpy, cupy, dask_array, dask_cupy)

### Added
- `numpy`, `cupy`, `dask.array` frameworks
- `to_numpy()`
- `to_ndarray()`
- `FrontEndNumpy`, `BackEndNumpy`
- `import vdf.numpy`
- `vdf.numpy.array(..., chunks=...)`
- `vdf.numpy.asnumpy(d)`
- `vdf.numpy.asdarray(d)`
- `vdf.numpy.save(d)`
- `vdf.numpy.load(d)`

### Changed
- Rename `FrontEnd` to `FrontEndPandas`
- Rename `BackEnd` to `BackEndPandas`

## [v0.5] - 2022-11-15

First version published on the repositories (pip and conda-forge).

### Added
- Pandas, cudf, modin, dask, dask_cudf, dask_modin, pyspark and pyspark+rapids frameworks

### Changed
- Nop

### Deprecated
- Nop

### Removed
- Nop

### Fixed
- Nop

### Security
- Nop

