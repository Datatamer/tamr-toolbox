"""Tests for tasks related creating and updating datasets in Tamr"""
import pytest

import tamr_toolbox as tbox
from tamr_unify_client.attribute.resource import AttributeSpec
from tamr_unify_client.dataset.resource import DatasetSpec
from tamr_unify_client.attribute.type import AttributeType

# from tamr_unify_client.attribute.type import AttributeType
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
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


def remove_test_datasets():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset_names = [
        DATASET_NAME,
        DATASET_NAME + "_multikey",
        DATASET_NAME + "_non_default_attribute",
    ]
    for dataset_name in dataset_names:
        if tbox.data_io.manage_dataset._check_dataset_exists(
            target_instance=client, dataset=dataset_name
        ):
            dataset = client.datasets.by_name(dataset_name)
            dataset.delete()
        assert not tbox.data_io.manage_dataset._check_dataset_exists(
            target_instance=client, dataset=dataset_name
        )


@mock_api(enforce_online_test=enforce_online_test)
def test_create_new_dataset():
    # Reset test datasets if they exist
    remove_test_datasets()
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address"]
    description = "My test dataset"
    dataset_spec = (
        DatasetSpec.new()
        .with_name(DATASET_NAME)
        .with_key_attribute_names(PRIMARY_KEYS)
        .with_description(description)
    )

    _ = tbox.data_io.manage_dataset.create_dataset(
        tamr=client, dataset_spec=dataset_spec, attributes=attributes,
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
def test_dataset_already_exists():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset_spec = DatasetSpec.new().with_name(DATASET_NAME).with_key_attribute_names(PRIMARY_KEYS)

    attributes = ["name", "address", "user_id", "account_number"]

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.create_dataset(
            tamr=client, dataset_spec=dataset_spec, attributes=attributes,
        )


@mock_api(enforce_online_test=enforce_online_test)
def test_create_multiple_pk():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["id", "source", "name", "address"]
    description = "My test dataset with two primary keys"
    primary_keys = ["id", "source"]
    dataset_name = DATASET_NAME + "_multikey"

    dataset_spec = (
        DatasetSpec.new()
        .with_name(dataset_name)
        .with_key_attribute_names(primary_keys)
        .with_description(description)
    )

    _ = tbox.data_io.manage_dataset.create_dataset(
        tamr=client, dataset_spec=dataset_spec, attributes=attributes,
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


@mock_api(enforce_online_test=enforce_online_test)
def test_create_dataset_w_attribute_specs():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    description = "My test dataset"
    dataset_name = DATASET_NAME + "_non_default_attribute"
    dataset_spec = (
        DatasetSpec.new()
        .with_name(dataset_name)
        .with_key_attribute_names(PRIMARY_KEYS)
        .with_description(description)
    )

    attribute_names = ["unique_id", "name", "address", "salary"]
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
            "innerType": {"baseType": "DOUBLE", "attributes": []},
            "attributes": [],
        },
    ]
    attribute_specs = []
    for idx in range(len(attribute_names)):
        name = attribute_names[idx]
        attribute_specs.append(
            AttributeSpec.new()
            .with_name(name)
            .with_type(AttributeType(attribute_types[idx]).spec())
        )

    _ = tbox.data_io.manage_dataset.create_dataset(
        tamr=client, dataset_spec=dataset_spec, attributes=attribute_specs,
    )

    dataset = client.datasets.by_name(dataset_name)

    assert dataset.description == description

    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names
    for idx in range(len(tamr_attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_add_default_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address", "phone"]
    dataset = client.datasets.by_name(DATASET_NAME)

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, attributes=attributes,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)

    dataset_attributes = updated_dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes


@mock_api(enforce_online_test=enforce_online_test)
def test_update_description():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    description = "My test dataset with phone"

    new_dataset_spec = dataset.spec().with_description(description)

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, new_dataset_spec=new_dataset_spec,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    assert updated_dataset.description == description


@mock_api(enforce_online_test=enforce_online_test)
def test_remove_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attributes = ["unique_id", "name", "address"]
    description = "My test dataset without phone"

    new_dataset_spec = dataset.spec().with_description(description)

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, new_dataset_spec=new_dataset_spec, attributes=attributes
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)

    assert updated_dataset.description == description

    dataset_attributes = updated_dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attributes)
    assert attribute_list == attributes


@mock_api(enforce_online_test=enforce_online_test)
def test_add_non_default_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)

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

    attribute_specs = []
    for idx in range(len(attribute_names)):
        name = attribute_names[idx]
        attribute_specs.append(
            AttributeSpec.new()
            .with_name(name)
            .with_type(AttributeType(attribute_types[idx]).spec())
        )

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, attributes=attribute_specs,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)

    dataset_attributes = updated_dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_add_primitive_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)

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

    attribute_specs = []
    for idx in range(len(attribute_names)):
        name = attribute_names[idx]
        attribute_specs.append(
            AttributeSpec.new()
            .with_name(name)
            .with_type(AttributeType(attribute_types[idx]).spec())
        )

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, attributes=attribute_specs,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    dataset_attributes = updated_dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]


@mock_api(enforce_online_test=enforce_online_test)
def test_modify_ud():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name("minimal_mastering_unified_dataset")
    attributes = [attr.spec() for attr in dataset.attributes.stream()]

    with pytest.raises(ValueError):
        _ = tbox.data_io.manage_dataset.modify_dataset(
            tamr=client, dataset=dataset, attributes=attributes,
        )


@mock_api(enforce_online_test=enforce_online_test)
def test_update_tags():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    tags = ["testing"]

    new_dataset_spec = dataset.spec().with_tags(tags)

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, new_dataset_spec=new_dataset_spec,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    assert updated_dataset.tags == tags


@mock_api()
def test_change_attribute_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
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

    attribute_specs = []
    for idx in range(len(attribute_names)):
        name = attribute_names[idx]
        attribute_specs.append(
            AttributeSpec.new()
            .with_name(name)
            .with_type(AttributeType(attribute_types[idx]).spec())
        )

    _ = tbox.data_io.manage_dataset.modify_dataset(
        tamr=client, dataset=dataset, attributes=attribute_specs,
    )

    dataset = client.datasets.by_name(DATASET_NAME)
    dataset_attributes = dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]
