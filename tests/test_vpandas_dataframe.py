import shutil
import tempfile
from pathlib import Path

import pandas
import pytest
from numpy import int64, int32

import virtual_dataframe as vdf
from virtual_dataframe import Mode, VDF_MODE
from virtual_dataframe import VClient


def test_dataframe_repartition(vclient):
    df = vdf.VDataFrame([1])
    rc = df.repartition(npartitions=1)
    assert rc.to_pandas().equals(df.to_pandas())


# %%
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_DataFrame_to_from_pandas():
    pdf = pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]}))


def test_DataFrame_compute():
    expected = pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]})
    result = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]})
    assert result.to_pandas().equals(expected)


def test_DataFrame_visualize():
    result = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]}).sort_values(by='a')
    result.visualize(filename=None)


@pytest.mark.filterwarnings("ignore:.*is a new feature!")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*will be removed in a future version")
def test_DataFrame_to_read_csv():
    d = tempfile.mkdtemp()
    try:
        with VClient():
            filename = f"{d}/test*.csv"
            df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
            df.to_csv(filename, index=False)
            df2 = vdf.read_csv(filename, dtype=int)
            assert df.to_pandas().sort_values("a").reset_index(drop=True).equals(
                df2.to_pandas().sort_values("a").reset_index(drop=True))

    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
@pytest.mark.filterwarnings("ignore:.*is a new feature!")
def test_DataFrame_to_read_excel():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.xlsx"

        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_excel(filename, index=False)
        df2 = vdf.read_excel(filename, dtype=int)
        assert df.to_pandas().sort_values(by='a').equals(df2.to_pandas().sort_values(by='a'))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.dask_array, Mode.dask_cudf, Mode.dask_cupy,
                                     Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
@pytest.mark.filterwarnings("ignore:.*this may be GPU accelerated in the future")
def test_DataFrame_to_read_feather():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.feather"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_feather(filename)
        df2 = vdf.read_feather(filename)
        assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
            df2.sort_values("a").to_backend().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy,
                                     Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_read_fwf():
    filename = f"tests/test*.fwf"
    df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
    df2 = vdf.read_fwf(filename, dtype=int)
    assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
        df2.sort_values("a").to_backend().reset_index(drop=True))


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask_cudf, Mode.dask_cupy, Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
@pytest.mark.filterwarnings("ignore:.*this may be GPU accelerated in the future")
def test_DataFrame_to_read_hdf():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.h5"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_hdf(filename, key='a', index=False)
        df2 = vdf.read_hdf(filename, key='a')
        assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
            df2.sort_values("a").to_backend().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
@pytest.mark.filterwarnings("ignore:.*this may be GPU accelerated in the future")
@pytest.mark.filterwarnings("ignore:.*will be removed in a future version")
def test_DataFrame_to_read_json():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.json"
        df = vdf.VDataFrame({'a': list(range(0, 10000)), 'b': list(range(0, 10000))}, npartitions=2)
        df.to_json(filename)
        df2 = vdf.read_json(filename)
        assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
            df2.sort_values("a").to_backend().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_parquet():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.parquet"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_parquet(filename)
        df2 = vdf.read_parquet(filename)
        assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
            df2.sort_values("a").to_backend().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_orc():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.orc"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        if "to_orc" not in dir(df):
            pytest.skip("unsupported to_orc")
        else:
            df.to_orc(filename)
            df2 = vdf.read_orc(filename, columns=("a", "b"))
            assert df.sort_values("a").to_backend().reset_index(drop=True).equals(
                df2.sort_values("a").to_backend().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.cupy, Mode.dask_cudf, Mode.dask_cupy,
                                     Mode.pyspark, Mode.pyspark_gpu),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_sql():
    filename = f"{tempfile.gettempdir()}/test.db"
    try:
        import sqlalchemy
        if VDF_MODE in (Mode.pyspark, Mode.pyspark_gpu):
            db_uri = f"jdbc:sqlite:{filename}"
        else:
            db_uri = f'sqlite:////{filename}'  # f"jdbc:sqlite:{filename}" for pyspark

        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df = df.set_index("a")
        df.to_sql('test_dataframe',
                  con=db_uri,
                  index_label="a",
                  if_exists='replace',
                  index=True)
        df2 = vdf.read_sql_table("test_dataframe",
                                 con=db_uri,
                                 index_col='a',  # For dask and dask_cudf
                                 )
        assert df.to_backend().equals(df2.to_backend())
    finally:
        Path(filename).unlink(missing_ok=True)
        pass


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_DataFrame_to_from_numpy():
    df = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0]}, npartitions=2)
    n = df.to_numpy()
    df2 = vdf.VDataFrame(n, columns=df.columns, npartitions=2)
    assert df.to_backend().equals(df2.to_backend())


def test_DataFrame_map_partitions():
    df = vdf.VDataFrame(
        {
            'a': [0.0, 1.0, 2.0, 3.0],
            'b': [1, 2, 3, 4],
        },
        npartitions=2
    )
    expected = pandas.DataFrame(
        {
            'a': [0.0, 1.0, 2.0, 3.0],
            'b': [1, 2, 3, 4],
            'c': [0.0, 20.0, 60.0, 120.0]
        }
    )
    # _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, **args, **kwargs)
    result = df.map_partitions(lambda df, v: df.assign(c=df.a * df.b * v), v=10)
    assert result.to_pandas().equals(expected)


def test_DataFrame_persist():
    df = vdf.VDataFrame(
        {
            'a': [0.0, 1.0, 2.0, 3.0],
            'b': [1, 2, 3, 4],
        },
        npartitions=2
    )
    df.persist()
    expected = pandas.DataFrame(
        {
            'a': [0.0, 1.0, 2.0, 3.0],
            'b': [1, 2, 3, 4],
        }
    )
    assert df.to_pandas().equals(expected)
