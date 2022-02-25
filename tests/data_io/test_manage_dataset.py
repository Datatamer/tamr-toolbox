"""Tests for tasks related creating and updating datasets in Tamr"""
import pytest

import tamr_toolbox as tbox
from tamr_unify_client.attribute.resource import Attribute

# from tamr_unify_client.attribute.type import AttributeType
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

# A valid password is not needed for offline tests, some value must be provided
CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
DATSET_NAME = "test_create_dataset"
PK = ["unique_id"]


@mock_api()
def test_create_new_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address"]
    description = "My test dataset"

    _ = tbox.data_io.manage_dataset.create_dataset(
        tamr=client,
        dataset_name=DATSET_NAME,
        attributes=attributes,
        primary_keys=PK,
        description=description,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

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


@mock_api()
def test_dataset_already_exists():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["name", "address", "user_id", "account_number"]

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.create_dataset(
            tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
        )


@mock_api()
def test_create_multiple_pk():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["id", "source", "name", "address"]
    description = "My test dataset with two pks"
    pks = ["id", "source"]
    dataset_name = DATSET_NAME + "_multikey"

    _ = tbox.data_io.manage_dataset.create_dataset(
        tamr=client,
        dataset_name=dataset_name,
        attributes=attributes,
        primary_keys=pks,
        description=description,
    )

    dataset = client.datasets.by_name(dataset_name)

    assert dataset.description == description

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


@mock_api()
def test_add_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address", "phone"]

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes


@mock_api()
def test_update_description():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address", "phone"]
    description = "My test dataset with phone"

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client,
        dataset_name=DATSET_NAME,
        attributes=attributes,
        primary_keys=PK,
        description=description,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

    assert dataset.description == description


@mock_api()
def test_remove_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address"]
    description = "My test dataset without phone"

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client,
        dataset_name=DATSET_NAME,
        attributes=attributes,
        primary_keys=PK,
        description=description,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes


@mock_api()
def test_add_non_default_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attribute_names = ["unique_id", "name", "address", "user_id"]
    attribute_types = [
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
            "innerType": {"baseType": "INT", "attributes": []},
            "attributes": [],
        },
    ]

    attributes = [
        Attribute(client=client, data={"name": attribute_names[idx], "type": attribute_types[idx]})
        for idx in range(len(attribute_names))
    ]

    description = "My test dataset with user int ids"

    dataset = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client,
        dataset_name=DATSET_NAME,
        attributes=attributes,
        primary_keys=PK,
        description=description,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == [a.name for a in attributes]

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]


@mock_api()
def test_add_primitive_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attribute_names = ["unique_id", "name", "address", "user_id", "sales_count"]
    attribute_types = [
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
            "innerType": {"baseType": "INT", "attributes": []},
            "attributes": [],
        },
        {"baseType": "INT", "attributes": []},
    ]

    attributes = [
        Attribute(client=client, data={"name": attribute_names[idx], "type": attribute_types[idx]})
        for idx in range(len(attribute_names))
    ]

    dataset = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
    )

    dataset = client.datasets.by_name(DATSET_NAME)
    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == [a.name for a in attributes]

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]


@mock_api()
def test_missing_primary_key():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["name", "address", "user_id", "account_number"]

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.create_dataset(
            tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
        )

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.modify_dataset(
            tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
        )


@mock_api()
def test_modify_ud():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["name", "address", "user_id", "account_number"]

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.create_dataset(
            tamr=client,
            dataset_name="minimal_mastering_unified_dataset",
            attributes=attributes,
            primary_keys=PK,
        )


@mock_api()
def test_add_tags():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address"]
    tags = ["testing"]

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK, tags=tags,
    )

    dataset = client.datasets.by_name(DATSET_NAME)

    assert dataset.tags == tags


@mock_api()
def test_change_attribute_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attribute_names = ["unique_id", "name", "address", "user_id", "sales_count"]
    attribute_types = [
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
            "innerType": {"baseType": "INT", "attributes": []},
            "attributes": [],
        },
        {"baseType": "DOUBLE", "attributes": []},
    ]

    attributes = [
        Attribute(client=client, data={"name": attribute_names[idx], "type": attribute_types[idx]})
        for idx in range(len(attribute_names))
    ]

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset_name=DATSET_NAME, attributes=attributes, primary_keys=PK,
    )

    dataset = client.datasets.by_name(DATSET_NAME)
    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == [a.name for a in attributes]

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]
