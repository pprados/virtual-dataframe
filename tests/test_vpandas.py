import shutil
import tempfile

import numpy as np
import pandas
import pytest

import virtual_dataframe as vdf


# def setup_module(module):
#     vdf.VClient()
#
# def teardown_module(module):
#     pass
#
@pytest.fixture(scope="session")
def vclient():
    return vdf.VClient()


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


def test_concat():
    vdf.VClient()
    rc = list(vdf.concat([
        vdf.VDataFrame([1]),
        vdf.VDataFrame([2])]).to_pandas()[0])
    assert rc == [1, 2]


def test_from_pandas():
    pdf = pandas.DataFrame({"a": [1, 2]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pdf)


def test_from_backend():
    odf = vdf.VDataFrame({"a": [1, 2]}, npartitions=2)
    assert vdf.from_backend(odf.to_backend(), npartitions=2).to_pandas().equals(
        vdf.VDataFrame({"a": [1, 2]}).to_pandas())


# %%
def test_DataFrame_to_from_pandas():
    pdf = pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pandas.DataFrame({'a': [0.0, 1.0, 2.0, 3.0], 'b': [0.1, 0.2, None, 0.3]}))


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


def test_DataFrame_to_read_csv():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.csv"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_csv(filename, index=False)
        df2 = vdf.read_csv(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.cudf, vdf.Mode.dask, vdf.Mode.dask_cudf), reason="Incompatible mode")
def test_DataFrame_to_read_excel():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.xlsx"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_excel(filename, index=False)
        df2 = vdf.read_excel(filename, dtype=int)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.dask, vdf.Mode.dask_cudf), reason="Incompatible mode")
def test_DataFrame_to_read_feather():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.feather"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_feather(filename)
        df2 = vdf.read_feather(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.cudf, vdf.Mode.dask_cudf), reason="Incompatible mode")
def test_DataFrame_read_fwf():
    filename = f"tests/test*.fwf"
    df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
    df2 = vdf.read_fwf(filename, dtype=int)
    assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.dask_cudf,), reason="Incompatible mode")
def test_DataFrame_to_read_hdf():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.h5"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_hdf(filename, key='a', index=False)
        df2 = vdf.read_hdf(filename, key='a')
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


def test_DataFrame_to_read_json():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.json"
        df = vdf.VDataFrame({'a': list(range(0, 10000)), 'b': list(range(0, 10000))}, npartitions=2)
        df.to_json(filename)
        df2 = vdf.read_json(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.pandas,
                                     vdf.Mode.dask_cudf,
                                     vdf.Mode.modin,
                                     vdf.Mode.dask_modin), reason="Incompatible mode")
def test_DataFrame_to_read_orc():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.orc"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_orc(filename)  # Bug with dask
        df2 = vdf.read_orc(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


def test_DataFrame_to_read_parquet():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test.parquet"
        df = vdf.VDataFrame({'a': list(range(0, 3)), 'b': list(range(0, 30, 10))}, npartitions=2)
        df.to_parquet(filename)
        df2 = vdf.read_parquet(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


@pytest.mark.skipif(vdf.VDF_MODE in (vdf.Mode.cudf, vdf.Mode.dask_cudf), reason="Incompatible mode")
def test_DataFrame_to_read_sql():
    d = tempfile.mkdtemp()
    try:
        import sqlalchemy
        filename = f"{d}/test.db"
        filename = f"/tmp/test.db"
        db_uri = f'sqlite://{filename}'
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
        assert df.to_pandas().equals(df2.to_pandas())
    finally:
        # shutil.rmtree(d)
        pass


def test_DataFrame_to_from_numpy():
    df = vdf.VDataFrame({'a': [0.0, 1.0, 2.0, 3.0]}, npartitions=2)
    n = df.to_numpy()
    df2 = vdf.VDataFrame(n, columns=df.columns, npartitions=2)
    assert df.compute().equals(df2.compute())


def test_Series_to_from_numpy():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0], npartitions=2)
    n = s.to_numpy()
    s2 = vdf.VSeries(n, npartitions=2)
    assert s.compute().equals(s2.compute())


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
