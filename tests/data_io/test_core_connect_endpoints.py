"""Tests for related to the Tamr auxiliary service DF-connect endpoints"""
from tamr_toolbox.data_io.core_connect import client
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir
import os

os.environ.setdefault("CONNECT_TOOLBOX_POSTGRES_JDBC", "not/real/url")
os.environ.setdefault("CONNECT_TOOLBOX_DB_PASSWORD", "not a real password")
os.environ.setdefault("TAMR_TOOLBOX_HOST", "1.2.3.4")
os.environ.setdefault("TAMR_TOOLBOX_PASSWORD", "not a real password")

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_ingest():
    my_ingest_connect = client.from_config(CONFIG)
    print(my_ingest_connect)
    assert client.ingest_dataset(
        my_ingest_connect,
        dataset_name="test_core_connect",
        query="select * from dataset.dataset_ns_current limit 10",
    )


@mock_api()
def test_ingest_set_pk():
    my_ingest_connect = client.from_config(CONFIG)
    assert client.ingest_dataset(
        my_ingest_connect,
        dataset_name="test_core_connect_2",
        query="select * from dataset.dataset_ns_current limit 10",
        primary_key="id",
    )


@mock_api()
def test_profile():
    my_profile_connect = client.from_config(CONFIG)
    assert client.profile_query_results(
        my_profile_connect,
        dataset_name="test_core_connect_profile",
        queries=[
            "select * from dataset.dataset_ns_current limit 100",
            "select * from dataset.attribute_ns_current limit 100",
        ],
    )