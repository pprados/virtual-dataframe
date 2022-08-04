import math
import shutil
import tempfile

import numpy
import numpy as np
import pandas
import pytest

import virtual_dataframe as vdf


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

def test_visualize():
    @vdf.delayed
    def f(i):
        return i

    assert vdf.visualize(f(42), f(50))


def test_concat():
    rc = list(vdf.concat([
        vdf.VDataFrame([1]),
        vdf.VDataFrame([2])]).to_pandas()[0])
    assert rc == [1, 2]


def test_from_pandas():
    pdf = pandas.DataFrame({"a": [1, 2]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pdf)


def test_from_backend():  # FIXME: le test n'est pas correct
    odf = vdf.VDataFrame({"a": [1, 2]}, npartitions=2)
    assert vdf.from_backend(odf.to_backend(),npartitions=2).to_pandas().equals(vdf.VDataFrame({"a": [1, 2]}).to_pandas())


# %%
def test_DataFrame_to_from_pandas():
    pdf = pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, None, 0.3]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, None, 0.3]}))


def test_Series_to_from_pandas():
    ps = pandas.Series([1, 2, 3, None, 4])
    s = vdf.from_pandas(ps, npartitions=2)
    assert s.to_pandas().equals(pandas.Series([1, 2, 3, None, 4]))


def test_DataFrame_compute():
    expected = pandas.DataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, 0.3, 0.4]})
    result = vdf.VDataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, 0.3, 0.4]})
    assert result.compute().to_pandas().equals(expected)


def test_Series_compute():
    expected = pandas.Series([1, 2, 3, None, 4])
    result = vdf.VSeries([1, 2, 3, None, 4])
    assert result.compute().to_pandas().equals(expected)

def test_DataFrame_visualize():
    result = vdf.VDataFrame({'a': [0, 1, 2, 3], 'b': [0.1, 0.2, 0.3, 0.4]})
    assert result.visualize()


def test_Series_visualize():
    result = vdf.VSeries([1, 2, 3, None, 4])
    assert result.visualize()


def test_DataFrame_to_from_csv():
    d = tempfile.mkdtemp()
    try:
        filename = f"{d}/test*.csv"
        df = vdf.VDataFrame({'a': [0, 1, 2, 3]}, npartitions=2)
        df.to_csv(filename, index=False)
        df2 = vdf.read_csv(filename)  # FIXME: vérifier avec des samples
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


def test_DataFrame_map_partitions():
    df = vdf.VDataFrame(
        {
            'a': [0, 1, 2, 3],
            'b': [1, 2, 3, 4],
        },
        npartitions=2
    )
    expected = pandas.DataFrame(
        {
            'a': [0, 1, 2, 3],
            'b': [1, 2, 3, 4],
            'c': [0, 20, 60, 120]
        }
    )
    # _VDataFrame.map_partitions = lambda self, func, *args, **kwargs: func(self, **args, **kwargs)
    result = df.map_partitions(lambda df, v: df.assign(c=df.a * df.b * v), v=10)
    assert result.to_pandas().equals(expected)


def test_Series_map_partitions():
    s = vdf.VSeries([0, 1, 2, 3],
                    npartitions=2
                    )
    expected = pandas.Series([0, 2, 4, 6])
    result = s.map_partitions(lambda s: s * 2).compute().to_pandas()
    assert result.equals(expected)


def test_apply_rows():
    df = vdf.VDataFrame(
        {'a': [0, 1, 2, 3],
         'b': [1, 2, 3, 4],
         'c': [10, 20, 30, 40]
         },
        npartitions=2
    )

    def my_kernel(a_s, b_s, c_s, val, out):  # Compilé pour Kernel GPU
        for i, (a, b, c) in enumerate(zip(a_s, b_s, c_s)):
            out[i] = (a + b + c) * val

    expected = pandas.DataFrame(
        {'a': [0, 1, 2, 3],
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
        cache_key=True,
        # pessimistic_nulls= True,  # FIXME: dask_cudf n'implemente pas ceci
    ).compute()
    assert r.to_pandas().equals(expected)
