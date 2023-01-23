"""Tests for the update_records function for dynamically modifying a dataset's records"""
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.dataset.manage import update_records
import tamr_toolbox as tbox

import pandas as pd
from tamr_unify_client import Client

from tests._common import get_toolbox_root_dir

CONFIG = tbox.utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

# Create mock data
DATASET_NAME = "mock_data_records"
test_data = {"id": ["0", "1"], "first_name": ["John", "Jane"], "last_name": ["Doe", "Doe"]}
test_data_df = pd.DataFrame(test_data)
tbox.data_io.dataframe.validate(test_data_df)

enforce_online_test = False


def remove_test_datasets(client: Client):
    dataset_names = [
        DATASET_NAME,
        DATASET_NAME + "_sample",
        DATASET_NAME + "_non_default_attribute",
        DATASET_NAME + "_dup",
        DATASET_NAME + "_no_attr",
    ]
    for dataset_name in dataset_names:
        if tbox.dataset.manage.exists(client=client, dataset_name=dataset_name):
            dataset = client.datasets.by_name(dataset_name)
            dataset.delete()
        assert not tbox.dataset.manage.exists(client=client, dataset_name=dataset_name)


@mock_api(enforce_online_test=enforce_online_test)
def test_update_records():
    client = tbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    # Reset test dataset if it exists
    remove_test_datasets(client=client)

    # Initialize the test dataset
    tbox.dataset.manage.create(
        client=client,
        dataset_name=DATASET_NAME,
        primary_keys=["id"],
        attributes=["first_name", "last_name"],
        description="Dataset to test record updates",
    )
    dataset = client.datasets.by_name(DATASET_NAME)
    dataset.upsert_from_dataframe(test_data_df, primary_key_name="id")

    # Function should raise a KeyError if an invalid attribute name is used in an upsert
    key_error_raised = False
    try:
        bad_update = [{"middle_name": "James"}]
        update_records(dataset, updates=bad_update, primary_keys=["0"], primary_key_name="id")
    except KeyError:
        key_error_raised = True
    assert key_error_raised

    # Function should raise a TypeError for an updates list with invalid entries
    type_error_raised = False
    try:
        bad_update = [{"first_name": "James"}, "delet"]
        update_records(dataset, updates=bad_update, primary_keys=["0", "1"], primary_key_name="id")
    except TypeError:
        type_error_raised = True
    assert type_error_raised

    # Function should raise a ValueError if updates and primary_keys have different lengths
    value_error_raised = False
    try:
        bad_update = [{"first_name": "James"}, "delete"]
        update_records(dataset, updates=bad_update, primary_keys=["0"], primary_key_name="id")
    except ValueError:
        value_error_raised = True
    assert value_error_raised

    # Test an update to the dataset
    updates = [
        {"first_name": "John", "last_name": "Roe"},
        "delete",
        {"first_name": "Jill", "last_name": "Smith"},
        {"first_name": "Jack", "last_name": "Brown"},
    ]
    update_records(
        dataset, updates=updates, primary_keys=["0", "1", "2", "3"], primary_key_name="id"
    )
    current_records = sorted(list(dataset.records()), key=lambda x: x["id"])
    assert current_records == [
        {"id": "0", "first_name": ["John"], "last_name": ["Roe"]},
        {"id": "2", "first_name": ["Jill"], "last_name": ["Smith"]},
        {"id": "3", "first_name": ["Jack"], "last_name": ["Brown"]},
    ]

    # Test the delete_all flag
    update_records(dataset, delete_all=True, primary_keys=["0", "3"], primary_key_name="id")
    current_records = sorted(list(dataset.records()), key=lambda x: x["id"])
    assert current_records == [{"id": "2", "first_name": ["Jill"], "last_name": ["Smith"]}]

    # Clean up the test dataset
    remove_test_datasets(client=client)
