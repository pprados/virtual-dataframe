import shutil
import tempfile
from pathlib import Path
from time import sleep

import numpy as np
import pandas
import pytest

import virtual_dataframe as vdf
from virtual_dataframe import Mode, VDF_MODE
from virtual_dataframe import VClient


@pytest.fixture(scope="session")
def vclient():
    local_cluster = vdf.VLocalCluster(
        scheduler_port=0,
        device_memory_limit="5g",
    )
    client = vdf.VClient(
        address=local_cluster,
    )
    client.__enter__()
    yield client
    client.__exit__(None, None, None)


# %%
def test_delayed(vclient):
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(f(42))[0] == 42


def test_compute(vclient):
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(f(42), f(50)) == (42, 50)


def test_visualize(vclient):
    @vdf.delayed
    def f(i):
        return i

    assert vdf.visualize(f(42), f(50))


def test_concat(vclient):
    rc = list(vdf.concat([
        vdf.VDataFrame([1]),
        vdf.VDataFrame([2])]).to_pandas()[0])
    assert rc == [1, 2]


def test_persist(vclient):
    df1 = vdf.VDataFrame([1])
    df2 = vdf.VDataFrame([2])

    rc1, rc2 = vdf.persist(df1, df2)
    assert rc1.to_pandas().equals(df1.to_pandas())
    assert rc2.to_pandas().equals(df2.to_pandas())


def test_dataframe_persist(vclient):
    df = vdf.VDataFrame([1])
    rc = df.persist()
    assert rc.to_pandas().equals(df.to_pandas())


def test_Series_persist(vclient):
    s = vdf.VSeries([1])
    rc = s.persist()
    assert rc.to_pandas().equals(s.to_pandas())


def test_dataframe_repartition(vclient):
    df = vdf.VDataFrame([1])
    rc = df.repartition(npartitions=1)
    assert rc.to_pandas().equals(df.to_pandas())


def test_Series_repartition(vclient):
    s = vdf.VSeries([1])
    rc = s.repartition(npartitions=1)
    assert rc.to_pandas().equals(s.to_pandas())


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_from_pandas():
    pdf = pandas.DataFrame({"a": [1, 2]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pdf)


def test_from_backend():
    odf = vdf.VDataFrame({"a": [1, 2]}, npartitions=2)
    assert vdf.from_backend(odf.to_backend(), npartitions=2).to_pandas().equals(
        vdf.VDataFrame({"a": [1, 2]}).to_pandas())


# %%
@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_DataFrame_to_from_pandas():
    pdf = pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]}))


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_from_pandas():
    ps = pandas.Series([1, 2, 3, None, 4])
    s = vdf.from_pandas(ps, npartitions=2)
    assert s.to_pandas().equals(pandas.Series([1, 2, 3, None, 4]))


def test_DataFrame_compute():
    expected = pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]})
    result = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]})
    assert result.compute().to_pandas().equals(expected)


def test_Series_compute():
    expected = pandas.Series([1, 2, 3, None, 4])
    result = vdf.VSeries([1, 2, 3, None, 4])
    assert result.compute().to_pandas().equals(expected)


def test_DataFrame_visualize():
    result = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, 0.3, 0.4]})
    assert result.visualize()


def test_Series_visualize():
    result = vdf.VSeries([1, 2, 3, None, 4])
    assert result.visualize()


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
            df2 = vdf.read_csv(filename)
            assert (df.sort_values("a").reset_index(drop=True).to_backend()
                    == df2.sort_values("a").reset_index(drop=True).to_backend()).all().to_backend().all()

    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.dask, Mode.dask_cudf), reason="Incompatible mode")
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
        assert (df.sort_values("a").reset_index(drop=True).to_backend()
                == df2.sort_values("a").reset_index(drop=True).to_backend()).all().to_backend().all()
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.dask_cudf, Mode.pyspark),
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
        assert (df.sort_values("a").reset_index(drop=True)
                == df2.sort_values("a").reset_index(drop=True)).all().to_backend().all()
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.dask_cudf, Mode.pyspark),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_read_fwf():
    filename = f"tests/test*.fwf"
    df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
    df2 = vdf.read_fwf(filename, dtype=int)
    assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask_cudf, Mode.pyspark),
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
        assert (df.sort_values("a").reset_index(drop=True)
                == df2.sort_values("a").reset_index(drop=True)).all().to_backend().all()
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
        assert (df.sort_values("a").reset_index(drop=True).to_backend()
                == df2.sort_values("a").reset_index(drop=True).to_backend()).all()[0]
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.pandas,
                                     Mode.dask_cudf,
                                     Mode.modin,
                                     Mode.dask_modin), reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_orc():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.orc"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_orc(filename)  # Bug with dask
        df2 = vdf.read_orc(filename)
        assert (df.sort_values("a").reset_index(drop=True).to_backend()
                == df2.sort_values("a").reset_index(drop=True).to_backend()).all()[0]
    finally:
        shutil.rmtree(d)


@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_parquet():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.parquet"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_parquet(filename)
        df2 = vdf.read_parquet(filename)
        assert (df.sort_values("a").reset_index(drop=True).to_backend()
                == df2.sort_values("a").reset_index(drop=True).to_backend()).all()[0]
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.dask_cudf, Mode.pyspark),
                    reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_DataFrame_to_read_sql():
    filename = f"{tempfile.gettempdir()}/test.db"
    try:
        import sqlalchemy
        if VDF_MODE == Mode.pyspark:
            db_uri = f"jdbc:sqlite:{filename}"
        else:
            db_uri = f'sqlite:////{filename}'  # f"jdbc:sqlite:{filename}" for pyspark

        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df = df.set_index("a")
        df.to_sql('test',
                  con=db_uri,
                  index_label="a",
                  if_exists='replace',
                  index=True)
        df2 = vdf.read_sql_table("test",
                                 con=db_uri,
                                 index_col='a',  # For dask and dask_cudf
                                 )
        assert df.to_backend().equals(df2.to_backend())
    finally:
        Path(filename).unlink(missing_ok=True)
        pass


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.dask_cudf), reason="Incompatible mode")
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


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.dask, Mode.cudf, Mode.dask_cudf),
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


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.pyspark,),
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


@pytest.mark.skipif(vdf.VDF_MODE in (Mode.cudf, Mode.dask_cudf, Mode.pyspark), reason="Incompatible mode")
@pytest.mark.filterwarnings("ignore:Function ")
@pytest.mark.filterwarnings("ignore:.*defaulting to pandas")
def test_Series_to_sql():
    filename = f"{tempfile.gettempdir()}/test.db"
    try:
        import sqlalchemy
        db_uri = f'sqlite:////{filename}'
        s = vdf.VSeries(list(range(0, 3)), npartitions=2)
        s.to_sql('test',
                 con=db_uri,
                 index_label="a",
                 if_exists='replace',
                 index=True)
    finally:
        Path(filename).unlink(missing_ok=True)
        pass


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_DataFrame_to_from_numpy():
    df = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0]}, npartitions=2)
    n = df.to_numpy()
    df2 = vdf.VDataFrame(n, columns=df.columns, npartitions=2)
    assert df.to_backend().equals(df2.to_backend())


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_Series_to_from_numpy():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0], npartitions=2)
    n = s.to_numpy()
    s2 = vdf.VSeries(n, npartitions=2)
    assert s.to_backend().equals(s2.to_backend())


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


def test_Series_map_partitions():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0],
                    npartitions=2
                    )
    expected = pandas.Series([0.0, 2.0, 4.0, 6.0])
    result = s.map_partitions(lambda s: s * 2).compute().to_pandas()
    assert result.equals(expected)


def test_apply_rows():
    df = vdf.VDataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40]
         },
        npartitions=2
    )

    def my_kernel(a_s, b_s, c_s, val, out):  # Compilé pour Kernel GPU
        for i, (a, b, c) in enumerate(zip(a_s, b_s, c_s)):
            out[i] = (a + b + c) * val

    expected = pandas.DataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40],
         'out': [33, 69, 105, 141]
         })
    r = df.apply_rows(
        my_kernel,
        incols={'a': 'a_s', 'b': 'b_s', 'c': 'c_s'},
        outcols={'out': np.int64},  # Va créer une place pour chaque row, pour le résultat
        kwargs={
            "val": 3
        },
        cache_key="abc",
    ).compute()
    assert r.to_pandas().equals(expected)
