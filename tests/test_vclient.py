import pytest

import virtual_dataframe.vclient as vclient
from virtual_dataframe import Mode, VDF_MODE


def test_panda():
    env = {
    }
    with vclient._new_VClient(mode=Mode.pandas, env=env) as client:
        assert type(client).__name__ == "_FakeClient"
        assert repr(client) == '<Client: in-process scheduler>'


def test_cudf():
    env = {
    }
    with vclient._new_VClient(mode=Mode.cudf, env=env) as client:
        assert type(client).__name__ == "_FakeClient"
        assert repr(client) == '<Client: in-process scheduler>'


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
def test_dask_debug():
    from distributed import LocalCluster

    with (vclient._new_VClient(mode=Mode.dask, env=dict(DEBUG="True"))) as client:
        assert isinstance(client.cluster, LocalCluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
def test_dask_cluster_gpu():
    from dask_cuda import LocalCUDACluster
    with (vclient._new_VClient(mode=Mode.dask_cudf, env=dict(VDF_CLUSTER="dask://localhost"))) as client:
        assert isinstance(client.cluster, LocalCUDACluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
def test_dask_no_cluster_gpu():
    from dask_cuda import LocalCUDACluster
    with (vclient._new_VClient(mode=Mode.dask_cudf, env=dict())) as client:
        assert isinstance(client.cluster, LocalCUDACluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
def test_dask_cluster_no_gpu():
    from distributed import LocalCluster
    with (vclient._new_VClient(mode=Mode.dask, env=dict(VDF_CLUSTER="dask://localhost"))) as client:
        assert isinstance(client.cluster, LocalCluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")

# def test_ray_no_cluster_modin(mocker):
#     ray_init = mocker.patch("ray.init")
#     with (vclient._new_VClient(mode=Mode.ray_modin, env=dict())) as client:
#         ray_init.assert_called_with()
#
#
# def test_ray_cluster_modin_localhost(mocker):
#     ray_init = mocker.patch("ray.init")
#     with (vclient._new_VClient(mode=Mode.ray_modin, env=dict(VDF_CLUSTER="ray://localhost"))) as client:
#         ray_init.assert_called_with(address='ray://localhost:10001')


# def test_ray_cluster_modin_auto(mocker):
#     ray_init = mocker.patch("ray.init")
#     with (vclient._new_VClient(mode=Mode.ray_modin, env=dict(VDF_CLUSTER="ray://auto"))) as client:
#         ray_init.assert_called_with(address="auto")
