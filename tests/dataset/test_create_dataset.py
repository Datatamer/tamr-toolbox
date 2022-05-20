"""Tests for tasks related creating and updating datasets in Tamr"""
import pytest

import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.models.attribute_type import (
    Array,
    STRING,
    DOUBLE,
)

from tamr_unify_client import Client

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


def remove_test_datasets(client: Client):
    dataset_names = [
        DATASET_NAME,
        DATASET_NAME + "_multikey",
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
def test_create_new_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    # Reset test datasets if they exist
    remove_test_datasets(client=client)

    attributes = ["unique_id", "name", "address"]
    description = "My test dataset"

    tbox.dataset.manage.create(
        client=client,
        dataset_name=DATASET_NAME,
        attributes=attributes,
        primary_keys=PRIMARY_KEYS,
        description=description,
    )

    dataset = client.datasets.by_name(DATASET_NAME)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes

    expected_attribute_types = [
        {"baseType": "STRING", "attributes": []},
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
    ]
    for idx in range(len(expected_attribute_types)):
        assert attribute_types[idx].spec().to_dict() == expected_attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_create_new_dataset_no_attr():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset_name = DATASET_NAME + "_no_attr"

    tbox.dataset.manage.create(
        client=client, dataset_name=dataset_name, primary_keys=PRIMARY_KEYS,
    )

    dataset = client.datasets.by_name(dataset_name)

    dataset_attributes = dataset.attributes
    attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    expected_attribute_types = [
        {"baseType": "STRING", "attributes": []},
    ]
    for idx in range(len(expected_attribute_types)):
        assert attribute_types[idx].spec().to_dict() == expected_attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_create_duplicate_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    attributes = ["unique_id", "name", "address"]
    description = "My test dataset"
    dataset_name = DATASET_NAME + "_dup"

    existing_dataset = client.datasets.by_name(DATASET_NAME)

    tbox.dataset.manage.create(client=client, dataset_name=dataset_name, dataset=existing_dataset)

    dataset = client.datasets.by_name(dataset_name)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes
    expected_attribute_types = [
        {"baseType": "STRING", "attributes": []},
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
    ]
    for idx in range(len(expected_attribute_types)):
        assert attribute_types[idx].spec().to_dict() == expected_attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_dataset_already_exists():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    attributes = ["name", "address", "user_id", "account_number"]

    with pytest.raises(ValueError):
        tbox.dataset.manage.create(
            client=client,
            dataset_name=DATASET_NAME,
            attributes=attributes,
            primary_keys=PRIMARY_KEYS,
        )


@mock_api(enforce_online_test=enforce_online_test)
def test_no_dataset_or_pk():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    with pytest.raises(ValueError):
        tbox.dataset.manage.create(
            client=client, dataset_name=DATASET_NAME,
        )


@mock_api(enforce_online_test=enforce_online_test)
def test_create_multiple_pk():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["id", "source", "name", "address"]
    description = "My test dataset with two primary keys"
    primary_keys = ["id", "source"]
    dataset_name = DATASET_NAME + "_multikey"

    tbox.dataset.manage.create(
        client=client,
        dataset_name=dataset_name,
        attributes=attributes,
        primary_keys=primary_keys,
        description=description,
    )

    dataset = client.datasets.by_name(dataset_name)

    assert dataset.description == description
    assert dataset.key_attribute_names == primary_keys

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes
    expected_attribute_types = [
        {"baseType": "STRING", "attributes": []},
        {"baseType": "STRING", "attributes": []},
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
    ]
    for idx in range(len(expected_attribute_types)):
        assert attribute_types[idx].spec().to_dict() == expected_attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_create_dataset_w_attribute_types():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    description = "My test dataset"
    dataset_name = DATASET_NAME + "_non_default_attribute"

    attribute_names = ["unique_id", "name", "address", "salary"]

    attribute_types = [STRING, Array(STRING), Array(STRING), Array(DOUBLE)]

    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    tbox.dataset.manage.create(
        client=client,
        dataset_name=dataset_name,
        attributes=attribute_names,
        attribute_types=attr_type_dict,
        primary_keys=PRIMARY_KEYS,
        description=description,
    )

    expected_attribute_types = [
        {"baseType": "STRING", "attributes": []},
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "DOUBLE", "attributes": []},
            "attributes": [],
        },
    ]

    dataset = client.datasets.by_name(dataset_name)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names
    for idx in range(len(tamr_attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == expected_attribute_types[idx]
