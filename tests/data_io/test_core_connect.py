"""Tests for related to the Tamr Core-connect"""
import pytest
import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tamr_unify_client import Client
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

DATASET_NAME = "test_core_connect"

enforce_online_test = False


def remove_test_datasets(client: Client):
    dataset_names = [DATASET_NAME + "_sample", DATASET_NAME + "_profile", DATASET_NAME]
    for dataset_name in dataset_names:
        if tbox.dataset.manage.exists(client=client, dataset_name=dataset_name):
            dataset = client.datasets.by_name(dataset_name)
            dataset.delete()
        assert not tbox.dataset.manage.exists(client=client, dataset_name=dataset_name)


@mock_api(enforce_online_test=enforce_online_test)
def test_jdbc_ingest():
    tamr_client = utils.client.create(**CONFIG["toolbox_test_instance"])
    jdbc_connect = CONFIG["core_connect"]["jdbc"]["ingest"]

    # Reset test datasets if they exist
    remove_test_datasets(client=tamr_client)

    # ingest data and retrieve metdata, then check if the ingested data exists
    tbox.data_io.core_connect.jdbc.jdbc_ingest(
        client=tamr_client,
        jdbc_connect=jdbc_connect,
        dataset_name="test_core_connect",
        query="select * from dataset.dataset_ns_current limit 10",
        retrieve_connect_metadata="true",
    )
    assert tbox.dataset.manage.exists(client=tamr_client, dataset_name="test_core_connect")

    # truncate the tamr dataset, then ingest data with primary key
    assert tbox.data_io.core_connect.jdbc.jdbc_ingest(
        client=tamr_client,
        jdbc_connect=jdbc_connect,
        dataset_name="test_core_connect",
        query="select * from dataset.dataset_ns_current limit 10",
        primary_key="id",
        truncate_tamr_dataset="true",
    )

    # check if error is raised correctly when tamr version doesn't meet the requirement
    with pytest.raises(Exception) as exc_info:
        assert tbox.data_io.core_connect.jdbc.jdbc_ingest(
            client=tamr_client,
            jdbc_connect=jdbc_connect,
            dataset_name="test_core_connect",
            query="select * from dataset.dataset_ns_current limit 10",
            tamr_min_version="2023.008.0",
        )
    assert exc_info.value.args[0] == "Core-connect is not available in current version of Tamr."

    # check if error is raised correctly when ingestion fails
    with pytest.raises(Exception) as exc_info:
        tbox.data_io.core_connect.jdbc.jdbc_ingest(
            client=tamr_client,
            jdbc_connect=jdbc_connect,
            dataset_name="test_core_connect",
            query="select * from dataset.dataset limit 10",
            primary_key="id",
            truncate_tamr_dataset="true",
        )
    assert {
        exc_info.value.args[0]
        == "org.postgresql.util.PSQLException: ERROR: relation 'dataset.dataset' does not exist\n"
        "  Position: 15"
    }
