import pandas
import pytest

import virtual_dataframe as vdf


def test_apply_rows(vclient):
    import numpy

    df = vdf.VDataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1., 2., 3., 4.],
         'c': [10., 20., 30., 40.]
         },
        npartitions=2
    )

    def my_kernel(a_s, b_s, c_s, val, out):  # Compilé pour Kernel GPU
        for i, (a, b, c) in enumerate(zip(a_s, b_s, c_s)):
            out[i] = int((a + b + c) * val)

    expected = pandas.DataFrame(
        {'a': [0.0, 1.0, 2.0, 3.0],
         'b': [1., 2., 3., 4.],
         'c': [10., 20., 30., 40.],
         'out': [33, 69, 105, 141]
         })
    r = df.apply_rows(
        my_kernel,
        incols={'a': 'a_s', 'b': 'b_s', 'c': 'c_s'},
        outcols={'out': numpy.int64},  # Va créer une place pour chaque row, pour le résultat
        kwargs={
            "val": 3
        },
        cache_key="abc",
    ).compute().to_pandas()
    assert r.equals(expected)


def test_delayed():
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(vdf.delayed(f)(42)) == (42,)


def test_compute(vclient):
    @vdf.delayed
    def f(i):
        return i

    assert vdf.compute(f(42), f(50)) == (42, 50)


def test_persist(vclient):
    @vdf.delayed
    def f(i):
        return i

    assert vdf.persist(f(42))[0] == 42


def test_visualize(vclient):
    @vdf.delayed
    def f(i):
        return i

    vdf.visualize(f(42), f(50), filename=None)


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


def test_from_backend(vclient):
    odf = vdf.VDataFrame({"a": [1, 2]}, npartitions=2)
    assert vdf.from_backend(odf.to_backend(), npartitions=2).to_pandas().equals(
        vdf.VDataFrame({"a": [1, 2]}).to_pandas())


@pytest.mark.filterwarnings("ignore:.*This may take some time.")
def test_from_pandas(vclient):
    pdf = pandas.DataFrame({"a": [1, 2]})
    df = vdf.from_pandas(pdf, npartitions=2)
    assert df.to_pandas().equals(pdf)
