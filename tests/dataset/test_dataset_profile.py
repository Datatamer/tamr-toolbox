"""Tests for tasks related creating and updating datasets in Tamr"""
import pytest
import pandas as pd

import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.dataset import get_profile
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.data_io import dataframe

from tamr_unify_client import Client

from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

# Create some mock data to upsert into a Tamr dataset:
DATASET_NAME = "mock_data_profile"
test_data = {'id': ['0', '1'],
             'first_name': ['John', 'Jane'],
             'last_name': ['Doe', 'Doe']}
test_data_df = pd.DataFrame(test_data)
dataframe.validate(test_data_df)

# Note that some test cases are dependent on previous ones
# So all tests must be set to enforce_online_test = True or all to False
enforce_online_test = False


def remove_test_datasets(client: Client):
    dataset_names = [
        DATASET_NAME + "_sample",
        DATASET_NAME,
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
def test_get_profile():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Reset test datasets if they exist:
    remove_test_datasets(client=client)

    # Create the blank dataset and create a profile:
    tbox.dataset.manage.create(
        client=client,
        dataset_name=DATASET_NAME,
        primary_keys=["id"],
        attributes=["first_name", "last_name"],
        description="Dataset to test profile creation",
    )

    dataset = client.datasets.by_name(DATASET_NAME)
    profile = get_profile(dataset, True)

    # Check that the profile has been created and is up-to-date:
    assert profile.dataset_name == DATASET_NAME
    assert profile.is_up_to_date is True

    # Upsert mock data into the dataset from the dataframe:
    dataset.upsert_from_dataframe(test_data_df, primary_key_name="id")

    # Get an unmodified profile:
    profile = get_profile(dataset, False)

    # Check that the profile is not up-to-date:
    assert profile.is_up_to_date is False

    # Get a refreshed profile:
    profile = get_profile(dataset, True)

    # Check that profile is again up-to-date:
    assert profile.is_up_to_date is True
