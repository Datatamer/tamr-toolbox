"""Tests for tasks related creating and updating datasets in Tamr"""
import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.models.attribute_type import Array, STRING, DOUBLE, INT

from tests._common import get_toolbox_root_dir

# A valid password is not needed for offline tests, some value must be provided
CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
DATASET_NAME = "test_create_dataset"
PRIMARY_KEYS = ["unique_id"]


# Note that some test cases are dependant on previous ones
# So all tests must be set to enforce_online_test = True or all to False
enforce_online_test = False


@mock_api(enforce_online_test=enforce_online_test)
def test_update_attribute_descriptions():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attribute_names = ["address", "user_id"]
    attribute_description = {
        "user_id": "The unique id for each sales rep",
        "address": "an address",
    }

    tbox.dataset.manage.edit_attributes(
        dataset=dataset,
        attribute_descriptions=attribute_description,
        override_existing_types=True,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    target_dataset_attributes = updated_dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr

    for attribute_name in attribute_names:
        updated_attr = target_attribute_dict[attribute_name]
        assert updated_attr.description == attribute_description[attribute_name]


@mock_api(enforce_online_test=enforce_online_test)
def test_remove_attribute_descriptions():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attribute_names = ["address", "user_id"]
    attribute_description = {
        "user_id": "",
        "address": "",
    }

    tbox.dataset.manage.edit_attributes(
        dataset=dataset,
        attribute_descriptions=attribute_description,
        override_existing_types=True,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    target_dataset_attributes = updated_dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr

    for attribute_name in attribute_names:
        updated_attr = target_attribute_dict[attribute_name]
        assert updated_attr.description == attribute_description[attribute_name]


@mock_api(enforce_online_test=enforce_online_test)
def test_remove_attribute_by_name():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attribute_name = "user_id"

    tbox.dataset.manage.delete_attributes(
        dataset=dataset, attributes=[attribute_name],
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    target_dataset_attributes = updated_dataset.attributes
    updated_attributes = []
    for attr in target_dataset_attributes.stream():
        updated_attributes.append(attr.name)

    assert attribute_name not in updated_attributes


def test_from_json():

    attribute_types = [
        {"baseType": "STRING", "attributes": []},
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "INT", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "DOUBLE", "attributes": []},
            "attributes": [],
        },
    ]

    converted_attr_types = [
        tbox.models.attribute_type.from_json(attr_type) for attr_type in attribute_types
    ]

    expected_attribute_types = [STRING, Array(STRING), Array(INT), Array(DOUBLE)]

    for i in range(len(converted_attr_types)):
        assert converted_attr_types[i] == expected_attribute_types[i]
