"""Tests for related to the Tamr Core-connect"""
import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tamr_unify_client import Client
from tests._common import get_toolbox_root_dir
import os

os.environ.setdefault("CONNECT_TOOLBOX_POSTGRES_JDBC", "jdbc:postgresql://localhost:5432/doit")
os.environ.setdefault("CONNECT_TOOLBOX_DB_PASSWORD", "12345")
os.environ.setdefault("TAMR_TOOLBOX_HOST", "10.20.0.250")
os.environ.setdefault("TAMR_TOOLBOX_PASSWORD", "8&j0DhqrLrOF")

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

DATASET_NAME = "test_core_connect"
PRIMARY_KEYS = None

enforce_online_test = True


def remove_test_datasets(client: Client):
    dataset_names = [
        DATASET_NAME,
        DATASET_NAME + "_2",
        DATASET_NAME + "_profile",
        DATASET_NAME + "_sample",
        DATASET_NAME + "_2_sample"
    ]
    for dataset_name in dataset_names:
        if tbox.dataset.manage.exists(client=client, dataset_name=dataset_name):
            dataset = client.datasets.by_name(dataset_name)
            dataset.delete()
        assert not tbox.dataset.manage.exists(client=client, dataset_name=dataset_name)


@mock_api(enforce_online_test=enforce_online_test)
def test_ingest_dataset():
    tamr_client = utils.client.create(**CONFIG["toolbox_test_instance"])
    jdbc_connect = CONFIG["core_connect"]["jdbc"]["ingest"]

    # Reset test datasets if they exist
    remove_test_datasets(client=tamr_client)

    tbox.data_io.core_connect.ingest_dataset(
        client=tamr_client,
        jdbc_connect=jdbc_connect,
        dataset_name=DATASET_NAME,
        query="select * from dataset.dataset_ns_current limit 10",
        primary_key=PRIMARY_KEYS,
        # truncate_tamr_dataset="true",
        retrieve_connect_metadata="true"
    )
