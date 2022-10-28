import os

import pytest

import virtual_dataframe.vclient as vclient
import virtual_dataframe.vlocalcluster as vlocalcluster
from virtual_dataframe import Mode, VDF_MODE


def test_panda():
    env = {
    }
    with vclient._new_VClient(
            mode=Mode.pandas,
            env=env,
    ) as client:
        assert type(client).__name__ == "_ClientDummy"
        assert repr(client) == '<Client: in-process scheduler>'


def test_cudf():
    env = {
    }
    with vclient._new_VClient(
            mode=Mode.cudf,
            env=env,
    ) as client:
        assert type(client).__name__ == "_ClientDummy"
        assert repr(client) == '<Client: in-process scheduler>'


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
def test_dask_debug():
    with vclient._new_VClient(
            mode=Mode.dask,
            env=dict(DEBUG="True"),
    ) as client:
        assert repr(client).startswith('<Client: in-process scheduler>')
        client.shutdown()


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
def test_dask_cudf_implicit_cluster():
    # Use implicit LocalCluster without parameters
    from dask_cuda import LocalCUDACluster
    os.environ["DASK_LOCAL__SCHEDULER_PORT"] = "0"
    os.environ["DASK_LOCAL__DEVICE_MEMORY_LIMIT"] = "1g"
    import dask
    dask.config.refresh()
    with vclient._new_VClient(
            mode=Mode.dask_cudf,
            env=dict(VDF_CLUSTER="dask://.local"),
    ) as client:
        assert isinstance(client.cluster, LocalCUDACluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
def test_dask_cudf_with_local_cluster():
    # Use explicit LocalCudaCluster with parameters
    from dask_cuda import LocalCUDACluster
    with \
            vlocalcluster._new_VLocalCluster(
                mode=Mode.dask_cudf,
                scheduler_port=0,
            ) as cluster, \
            vclient._new_VClient(
                mode=Mode.dask_cudf,
                env={},
                address=cluster,
            ) as client:
        assert isinstance(cluster, LocalCUDACluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
def test_dask_cudf_with_default_cluster():
    with vclient._new_VClient(
            mode=Mode.dask_cudf,
            env=dict(),
    ) as client:
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")
        client.shutdown()


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
def test_dask_implicit_cluster():
    from dask.distributed import LocalCluster
    os.environ["DASK_LOCAL__SCHEDULER_PORT"] = "0"
    os.environ["DASK_LOCAL__DEVICE_MEMORY_LIMIT"] = "1g"
    import dask
    dask.config.refresh()
    with vclient._new_VClient(
            mode=Mode.dask,
            env=dict(VDF_CLUSTER="dask://.local"),
    ) as client:
        assert isinstance(client.cluster, LocalCluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")
        client.shutdown()


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
def test_dask_with_local_cluster():  # TODO: Mock
    from dask.distributed import LocalCluster
    with \
            vlocalcluster._new_VLocalCluster(
                mode=Mode.dask,
                scheduler_port=0,
            ) as cluster, \
            vclient._new_VClient(
                mode=Mode.dask,
                env={},
                address=cluster,
            ) as client:
        assert isinstance(client.cluster, LocalCluster)
        assert repr(client).startswith("<Client: 'tcp://127.0.0.1:")
        client.shutdown()

# FIXME
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

@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
def test_pyspark_implicit_cluster():
    import pyspark
    with vclient._new_VClient(
            mode=Mode.pyspark,
            env={},
    ) as client:
        assert isinstance(client.session, pyspark.sql.session.SparkSession)
        assert repr(client) == "<Spark: 'local[*]'>"
        client.shutdown()


