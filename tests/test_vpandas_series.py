import shutil
import tempfile
from pathlib import Path

import pandas
import pytest

import virtual_dataframe as vdf
from virtual_dataframe import Mode


def test_Series_persist(vclient):
    s = vdf.VSeries([1])
    rc = s.persist()
    assert rc.to_pandas().equals(s.to_pandas())


def test_Series_repartition(vclient):
    s = vdf.VSeries([1])
    rc = s.repartition(npartitions=1)
    assert rc.to_pandas().equals(s.to_pandas())


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_from_pandas():
    ps = pandas.Series([1, 2, 3, None, 4])
    s = vdf.from_pandas(ps, npartitions=2)
    assert s.to_pandas().equals(pandas.Series([1, 2, 3, None, 4]))


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy), reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_Series_to_csv():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.csv"
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_csv(filename)
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_excel():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.xlsx"
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_excel(filename)
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
@pytest.mark.filterwarnings("ignore:.*this may be GPU accelerated in the future")
def test_Series_to_hdf():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.hdf"
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_hdf(filename, key="a")
    finally:
        shutil.rmtree(d)


@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:Using CPU via Pandas")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_json():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.json"
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_json(filename)
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy,
                                     Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_Series_to_sql():
    filename = f"{tempfile.gettempdir()}/test.db"
    try:
        import sqlalchemy
        db_uri = f'sqlite:////{filename}'
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_sql('test_series',
                 con=db_uri,
                 index_label="a",
                 if_exists='replace',
                 index=True)
    finally:
        Path(filename).unlink(missing_ok=True)
        pass


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_from_numpy():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0], npartitions=2)
    n = s.to_numpy()
    s2 = vdf.VSeries(n, npartitions=2)
    assert s.to_backend().equals(s2.to_backend())


def test_Series_map_partitions():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0],
                    npartitions=2
                    )
    expected = pandas.Series([0.0, 2.0, 4.0, 6.0])
    result = s.map_partitions(lambda s: s * 2).compute().to_pandas()
    assert result.equals(expected)


def test_Series_compute():
    expected = pandas.Series([1, 2, 3, None, 4])
    result = vdf.VSeries([1, 2, 3, None, 4])
    assert result.compute().to_pandas().equals(expected)


def test_Series_visualize():
    result = vdf.VSeries([1, 2, 3, None, 4])
    result.visualize(filename=None)


def test_Series_persist():
    expected = pandas.Series([1, 2, 3, None, 4])
    result = vdf.VSeries([1, 2, 3, None, 4])
    result.persist()
    assert result.compute().to_pandas().equals(expected)
