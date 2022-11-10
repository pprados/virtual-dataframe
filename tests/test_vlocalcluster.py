from unittest.mock import patch

import virtual_dataframe.vlocalcluster as vlocalcluster
from virtual_dataframe import Mode


def test_panda():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.pandas)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"


def test_cudf():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.cudf)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"


@patch('dask.distributed.LocalCluster')
def test_dask(mockLocalCluster):
    vlocalcluster._new_VLocalCluster(mode=Mode.dask)
    assert mockLocalCluster.called_with()


def test_modin():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.modin)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"


@patch('dask.distributed.LocalCluster')
def test_dask_modin(mockLocalCluster):
    vlocalcluster._new_VLocalCluster(mode=Mode.dask_modin)
    assert mockLocalCluster.called_with()


@patch('dask_cuda.LocalCUDACluster')
def test_dask_cudf(mockLocalCUDACluster):
    vlocalcluster._new_VLocalCluster(mode=Mode.dask_cudf)
    mockLocalCUDACluster.assert_called_with()


def test_pyspark():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.pyspark)
    assert type(local_cluster).__name__ == "SparkLocalCluster"
    assert local_cluster.conf.get("spark.master") == "local[*]"
