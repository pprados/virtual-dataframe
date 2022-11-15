import os
from time import sleep
from unittest.mock import patch

import pytest

import virtual_dataframe.vclient as vclient
from virtual_dataframe import Mode, VDF_MODE, vlocalcluster


def test_panda():
    env = {
    }
    with vclient._new_VClient(
            mode=Mode.pandas,
            env=env,
            address=None,
    ) as client:
        assert type(client).__name__ == "_ClientDummy"
        assert repr(client) == '<Client: in-process scheduler>'


def test_cudf():
    env = {
    }
    with vclient._new_VClient(
            mode=Mode.cudf,
            env=env,
            address=None,
    ) as client:
        assert type(client).__name__ == "_ClientDummy"
        assert repr(client) == '<Client: in-process scheduler>'


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
@patch('virtual_dataframe.vclient._ClientDummy')
def test_dask_debug(mockClient):
    with vclient._new_VClient(
            mode=Mode.dask,
            env=dict(DEBUG="True"),
            address=None,
    ):
        assert mockClient.called
        assert mockClient.call_args == (('threads',), {})


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
@patch('dask.distributed.Client')
@patch('dask_cuda.LocalCUDACluster')
def test_dask_cudf_implicit_cluster(mockClient, mockLocalCUDACluster):
    os.environ["DASK_LOCAL__SCHEDULER_PORT"] = "0"
    os.environ["DASK_LOCAL__DEVICE_MEMORY_LIMIT"] = "1g"
    import dask
    dask.config.refresh()
    with vclient._new_VClient(
            mode=Mode.dask_cudf,
            env=dict(VDF_CLUSTER="dask://.local"),
            address=None,
    ):
        assert mockClient.called
        assert mockLocalCUDACluster.called


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
@patch('dask.distributed.Client')
@patch('dask_cuda.LocalCUDACluster')
def test_dask_cudf_with_local_cluster(mockClient, mockLocalCUDACluster):
    # Use explicit LocalCudaCluster with parameters
    with \
            vlocalcluster._new_VLocalCluster(
                mode=Mode.dask_cudf,
                scheduler_port=0,
            ) as cluster, \
            vclient._new_VClient(
                mode=Mode.dask_cudf,
                env={},
                address=cluster,
            ):
        assert mockClient.called
        assert mockLocalCUDACluster.called


@pytest.mark.skipif(VDF_MODE != Mode.dask_cudf, reason="Invalid mode")
@patch('dask.distributed.Client')
@patch('dask_cuda.LocalCUDACluster')
def test_dask_cudf_with_default_cluster(mockClient, mockLocalCUDACluster):
    with vclient._new_VClient(
            mode=Mode.dask_cudf,
            env=dict(),
            address=None,
    ):
        assert mockClient.called
        assert mockLocalCUDACluster.called


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
@patch('dask.distributed.Client')
@patch('dask.distributed.LocalCluster')
def test_dask_implicit_cluster(mockClient, mockLocalCluster):
    os.environ["DASK_LOCAL__SCHEDULER_PORT"] = "0"
    os.environ["DASK_LOCAL__DEVICE_MEMORY_LIMIT"] = "1g"
    import dask
    dask.config.refresh()
    with vclient._new_VClient(
            mode=Mode.dask,
            env=dict(VDF_CLUSTER="dask://.local"),
            address=None,
    ) as client:
        assert mockClient.called
        assert mockLocalCluster.called


@pytest.mark.skipif(not VDF_MODE.name.startswith("dask"), reason="Invalid mode")
@patch('dask.distributed.Client')
@patch('dask.distributed.LocalCluster')
def test_dask_with_local_cluster(mockClient, mockLocalCluster):
    with \
            vlocalcluster._new_VLocalCluster(
                mode=Mode.dask,
                scheduler_port=0,
            ) as cluster, \
            vclient._new_VClient(
                mode=Mode.dask,
                env={},
                address=cluster,
            ):
        assert mockClient.called
        assert mockLocalCluster.called


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_implicit_cluster(mockBuilder):
    with vclient._new_VClient(
            mode=Mode.pyspark,
            address=None,
            env={},
    ):
        assert mockBuilder.called
        assert mockBuilder.call_count == 1


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_address_cluster(mockBuilder):
    with vclient._new_VClient(
            mode=Mode.pyspark,
            address="local[*]",
            env={},
    ):
        assert mockBuilder.called
        master_call = next(filter(lambda c: len(c.args) and c.args[0] == 'spark.master', mockBuilder.mock_calls)).args
        assert master_call[1] == 'local[*]'


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_local_cluster(mockBuilder):
    with vclient._new_VClient(
            mode=Mode.pyspark,
            address=None,
            env={"VDF_CLUSTER": "spark:local[*]"},
    ):
        assert mockBuilder.called
        master_call = next(filter(lambda c: len(c.args) and c.args[0] == 'spark.master', mockBuilder.mock_calls)).args
        assert master_call[1] == 'local[*]'


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_dot_local_cluster(mockBuilder):
    with vclient._new_VClient(
            mode=Mode.pyspark,
            address=None,
            env={"VDF_CLUSTER": "spark://.local"},
    ):
        assert mockBuilder.called
        master_call = next(filter(lambda c: len(c.args) and c.args[0] == 'spark.master', mockBuilder.mock_calls)).args
        assert master_call[1] == 'local[*]'


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_master_env_cluster(mockBuilder):
    with vclient._new_VClient(
            mode=Mode.pyspark,
            address=None,
            env={
                "SPARK_MASTER_HOST": "local[*]",
            },
    ):
        assert mockBuilder.called
        master_call = next(filter(lambda c: len(c.args) and c.args[0] == 'spark.master', mockBuilder.mock_calls)).args
        assert master_call[1] == 'local[*]'


@pytest.mark.skipif(VDF_MODE != Mode.pyspark, reason="Invalid mode")
@patch('pyspark.sql.session.SparkSession.Builder.config')
def test_pyspark_vlocalcluster(mockBuilder):
    with \
            vlocalcluster._new_VLocalCluster(
                mode=Mode.pyspark,
                spark_app_name="toto",
                spark_master="local[*]",
            ) as cluster, \
            vclient._new_VClient(
                mode=Mode.pyspark,
                env={},
                address=cluster,
            ) as client:
        assert mockBuilder.called
        master_call = next(filter(lambda c: len(c.args) and c.args[0] == 'spark.master', mockBuilder.mock_calls)).args
        assert master_call[1] == 'local[*]'


def test_client_with_local_cluster():
    import virtual_dataframe
    with virtual_dataframe.VLocalCluster() as local_cluster,\
            virtual_dataframe.VClient(local_cluster):
        # Now, use the scheduler
        pass
