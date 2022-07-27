import shutil
import tempfile

import pandas

import virtual_dataframe as vdf
import virtual_dataframe.vpandas as vpd


# %%
def test_delayed():
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(f(42))[0] == 42


def test_compute():
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(f(42), f(50)) == (42, 50)


def test_concat():
    rc = list(vdf.concat([
        vdf.VDataFrame([1]),
        vdf.VDataFrame([2])]).to_pandas()[0])
    assert rc == [1, 2]


def test_from_pandas():
    pdf = pandas.DataFrame({"a": [1, 2]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pdf)


def test_from_virtual():
    odf = vdf.VDataFrame({"a": [1, 2]}, npartitions=2)
    assert odf.to_pandas().equals(vdf.VDataFrame({"a": [1, 2]}).to_pandas())


# %%
def test_DataFrame_to_from_pandas():
    pdf = pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, None, 0.3]})
    df = vpd.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, None, 0.3]}))


def test_Series_to_from_pandas():
    ps = pandas.Series([1, 2, 3, None, 4])
    s = vpd.from_pandas(ps, npartitions=2)
    assert s.to_pandas().equals(pandas.Series([1, 2, 3, None, 4]))


def test_DataFrame_compute():
    expected = pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, 0.3, 0.4]})
    result = vdf.VDataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, 0.3, 0.4]}).to_pandas()
    assert result.equals(expected)


def test_Series_compute():
    expected = pandas.Series([1, 2, 3, None, 4])
    result = vdf.VSeries([1, 2, 3, None, 4]).to_pandas()
    assert result.to_pandas().equals(expected)


def test_DataFrame_to_from_csv():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.csv"
        df = vdf.VDataFrame({'a': [0, 1, 2, 3]}, npartitions=2)
        df.to_csv(filename, index=False)
        df2 = vdf.read_csv(filename)
        assert df.to_pandas().reset_index(drop=True).equals(df2.to_pandas().reset_index(drop=True))
    finally:
        shutil.rmtree(d)


def test_DataFrame_to_from_numpy():
    df = vdf.VDataFrame({'a': [0, 1, 2, 3]}, npartitions=2)
    n = df.to_numpy()
    df2 = vdf.VDataFrame(n, columns=df.columns, npartitions=2)
    assert df.compute().equals(df2.compute())


def test_Series_to_from_numpy():
    s = vdf.VSeries([0.0, 1.0, 2.0, 3.0], npartitions=2)
    n = s.to_numpy()
    s2 = vdf.VSeries(n, npartitions=2)
    assert s.compute().equals(s2.compute())
