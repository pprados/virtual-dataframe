import pytest

import virtual_dataframe.vlocalcluster as vlocalcluster
from virtual_dataframe import Mode, VDF_MODE


def test_panda():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.pandas)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"


def test_cudf():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.cudf)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"

def test_dask():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.dask)
    assert type(local_cluster).__name__ == "LocalCluster"
    assert local_cluster.scheduler_address.startswith("tcp://127.0.0.1:")

def test_modin():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.modin)
    assert type(local_cluster).__name__ == "_LocalClusterDummy"
    assert repr(local_cluster) == "LocalClusterDummy('localhost:8786')"


def test_dask_modin():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.dask_modin)
    assert type(local_cluster).__name__ == "LocalCluster"
    assert local_cluster.scheduler_address.startswith("tcp://127.0.0.1:")

def test_dask_cudf():
    local_cluster = vlocalcluster._new_VLocalCluster(mode=Mode.dask_cudf)
    assert type(local_cluster).__name__ == "LocalCUDACluster"
    assert local_cluster.scheduler_address.startswith("tcp://127.0.0.1:")




