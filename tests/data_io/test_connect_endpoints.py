"""Tests for related to the Tamr auxiliary service DF-connect endpoints"""
from tamr_toolbox.data_io.df_connect import client
from tamr_toolbox.data_io.file_system_type import FileSystemType
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir
import os

os.environ.setdefault("CONNECT_TOOLBOX_POSTGRES_JDBC", "/not/a/real/url")
os.environ.setdefault("TAMR_TOOLBOX_HOST", "1.2.3.4.5")
CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_ingest():
    my_ingest_connect = client.from_config(CONFIG)
    print(my_ingest_connect)
    assert client.ingest_dataset(
        my_ingest_connect,
        dataset_name="test_df_connect",
        query="select * from dataset.dataset_ns_current limit 1",
    )


@mock_api()
def test_ingest_set_pk():
    my_ingest_connect = client.from_config(CONFIG)
    assert client.ingest_dataset(
        my_ingest_connect,
        dataset_name="test_df_connect_2",
        query="select * from dataset.dataset_ns_current limit 1",
        primary_key="id",
    )


@mock_api()
def test_export():
    my_export_connect = client.from_config(CONFIG)
    assert client.export_dataset(
        my_export_connect,
        dataset_name="test_df_connect",
        target_table_name="test",
        truncate_before_load=True,
    )


@mock_api()
def test_execute():
    my_export_connect = client.from_config(CONFIG)
    assert client.execute_statement(my_export_connect, "select * from test")


@mock_api()
def test_profile():
    my_profile_connect = client.from_config(CONFIG)
    assert client.profile_query_results(
        my_profile_connect,
        dataset_name="test_df_connect_profile",
        queries=[
            "select * from dataset.dataset_ns_current limit 100",
            "select * from dataset.attribute_ns_current limit 100",
        ],
    )


@mock_api()
def test_local_fs_avro_export():
    my_export_connect = client.from_config(CONFIG)
    assert client.export_dataset_as_avro(
        my_export_connect,
        url="/home/ubuntu/connect_export/data/people_tiny.avro",
        dataset_name="people_tiny.csv",
        fs_type=FileSystemType.LOCAL,
    )


@mock_api()
def test_local_fs_avro_schema_export():
    my_export_connect = client.from_config(CONFIG)
    assert client.export_dataset_avro_schema(
        my_export_connect,
        url="/home/ubuntu/connect_export/schema/people_tiny.avsc",
        dataset_name="people_tiny.csv",
        fs_type=FileSystemType.LOCAL,
    )
