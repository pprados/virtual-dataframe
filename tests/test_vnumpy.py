import shutil
import tempfile

import numpy
import numpy as np
import pytest

import virtual_dataframe as vdf
import virtual_dataframe.numpy as vnp
from virtual_dataframe import Mode, VDF_MODE


def test_array():
    assert numpy.array_equal(
        vnp.asnumpy(vnp.array([
            [0.0, 1.0, 2.0, 3.0],
            [1, 2, 3, 4],
            [10, 20, 30, 40]
        ])),
        np.array([
            [0.0, 1.0, 2.0, 3.0],
            [1, 2, 3, 4],
            [10, 20, 30, 40]
        ])
    )


def test_DataFrame_to_ndarray():
    df = vdf.VDataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40]
         },
        npartitions=2
    )
    a = df.to_ndarray()
    if VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy):
        import cupy
        assert isinstance(a, cupy.ndarray)
    else:
        assert isinstance(a, numpy.ndarray)
    assert numpy.array_equal(
        vnp.asnumpy(a),
        np.array([
            [0.0, 1.0, 2.0, 3.0],
            [1, 2, 3, 4],
            [10, 20, 30, 40]
        ]).T)


def test_Series_to_ndarray():
    serie = vdf.VSeries(
        [0.0, 1.0, 2.0, 3.0],
        npartitions=2
    )
    a = serie.to_ndarray()
    if VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy):
        import cupy
        assert isinstance(a, cupy.ndarray)
    else:
        assert isinstance(a, numpy.ndarray)
    assert numpy.array_equal(
        vnp.asnumpy(a),
        np.array(
            [0.0, 1.0, 2.0, 3.0],
        ))


def test_asarray():
    df = vdf.VDataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40]
         },
        npartitions=2
    )
    a = vnp.asarray(df['a'])
    assert numpy.array_equal(
        vnp.asnumpy(a),
        np.array(
            [0.0, 1.0, 2.0, 3.0],
        ))


def test_asndarray():
    df = vdf.VDataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40]
         },
        npartitions=2
    )
    a = vnp.asndarray(df['a'])
    if VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy):
        import cupy
        assert isinstance(a, cupy.ndarray)
    else:
        assert isinstance(a, numpy.ndarray)
    assert numpy.array_equal(
        vnp.asnumpy(a),
        np.array(
            [0.0, 1.0, 2.0, 3.0],
        ))


def test_DataFrame_ctr(vclient):
    a = vnp.array([
        [0.0, 1.0, 2.0, 3.0],
        [1, 2, 3, 4],
        [10, 20, 30, 40]
    ])
    df1 = vdf.VDataFrame(vdf.compute(a.T)[0])
    df2 = vdf.VDataFrame(
        {
            0: [0.0, 1.0, 2.0, 3.0],
            1: [1.0, 2.0, 3.0, 4.0],
            2: [10.0, 20.0, 30.0, 40.0]
        }
    )
    assert (df1.to_pandas() == df2.to_pandas()).all().all()


def test_ctr_serie():
    a = vnp.array([0.0, 1.0, 2.0, 3.0])
    vdf.VSeries(a)


def test_empty():
    vnp.compute(vnp.empty((3, 4)))[0]


def test_slicing():
    assert vnp.compute(vnp.array([1, 2])[1:])[0], "can not call compute()"


def test_asnumpy():
    assert isinstance(vnp.asnumpy(vnp.array([1, 2])), numpy.ndarray)


# %% chunks
def test_compute_chunk_sizes():
    vnp.compute_chunk_sizes(vnp.arange(100_000, chunks=(100,)))


def test_rechunk():
    vnp.rechunk(vnp.arange(100_000, chunks=(100,)), (200, 200))


# %% Random
def test_random_random():
    vnp.random.random((10000, 10000), chunks=(1000, 1000))


def test_random_binomial():
    vnp.random.binomial(10, .5, 1000, chunks=10)


def test_random_normal():
    vnp.random.normal(0, .1, 1000, chunks=10)


def test_random_poisson():
    vnp.random.poisson(5, 10000, chunks=100)


# %% from_...
def test_from_array():
    data = np.arange(100_000).reshape(200, 500)
    vnp.compute(vnp.from_array(data, chunks=(100, 100)))[0]


def test_save_and_load_npy():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.npy"
        d1 = vnp.arange(100_000).reshape(200, 500)
        vnp.save(filename, d1)
        d2 = vnp.load(filename)
        assert vnp.compute((d1 == d2).all())
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy),
                    reason="Incompatible mode")
def test_savez_and_load_npz():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.npz"
        d1 = vnp.arange(1_000).reshape(10, 100)
        vnp.savez(filename, arr=d1, allow_pickle=False)
        npz = vnp.load(filename, mmap_mode=False)
        d2 = npz["arr"]
        assert vnp.compute((d1 == d2).all())
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy),
                    reason="Incompatible mode")
def test_savez_compressed_and_load_npz():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.npz"
        d1 = vnp.arange(1_000).reshape(10, 100)
        vnp.savez_compressed(filename, arr=d1, allow_pickle=False)
        npz = vnp.load(filename, mmap_mode=False, allow_pickle=False)
        d2 = npz["arr"]
        assert vnp.array_equal(d1, d2)
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy),
                    reason="Incompatible mode")
def test_savetxt_and_loadtxt():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.txt"
        d1 = vnp.arange(1_000).reshape(10, 100)
        vnp.savetxt(filename, d1, delimiter=',', fmt='%1.4e')
        d2 = vnp.loadtxt(filename, delimiter=',')
        assert vnp.array_equal(d1, d2)
    finally:
        shutil.rmtree(d)
