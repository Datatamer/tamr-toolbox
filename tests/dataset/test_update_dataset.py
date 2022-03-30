"""Tests for tasks related creating and updating datasets in Tamr"""
import pytest

import tamr_toolbox as tbox
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


@mock_api(enforce_online_test=enforce_online_test)
def test_add_default_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    attributes = ["unique_id", "name", "address", "phone"]
    dataset = client.datasets.by_name(DATASET_NAME)

    tbox.dataset.manage.update(
        dataset=dataset, attributes=attributes,
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

    tbox.dataset.manage.update(
        dataset=dataset, description=description,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    assert updated_dataset.description == description


@mock_api(enforce_online_test=enforce_online_test)
def test_remove_attribute():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attributes = ["unique_id", "name", "address"]
    description = "My test dataset without phone"

    tbox.dataset.manage.update(dataset=dataset, attributes=attributes, description=description)

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
    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    tbox.dataset.manage.update(
        dataset=dataset, attributes=attribute_names, attribute_types=attr_type_dict,
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
    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    tbox.dataset.manage.update(
        dataset=dataset, attributes=attribute_names, attribute_types=attr_type_dict,
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
def test_update_ud():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name("minimal_mastering_unified_dataset")

    with pytest.raises(ValueError):
        tbox.dataset.manage.update(dataset=dataset,)


@mock_api(enforce_online_test=enforce_online_test)
def test_update_tags():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    tags = ["testing"]

    tbox.dataset.manage.update(
        dataset=dataset, tags=tags,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    assert updated_dataset.tags == tags


@mock_api(enforce_online_test=enforce_online_test)
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
    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    tbox.dataset.manage.update(
        dataset=dataset, attributes=attribute_names, attribute_types=attr_type_dict,
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
def test_partially_define_types():
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
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        },
    ]
    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    # remove some types defs
    del attr_type_dict["name"]
    del attr_type_dict["sales_count"]

    tbox.dataset.manage.update(
        dataset=dataset, attributes=attribute_names, attribute_types=attr_type_dict,
    )

    attr_type_dict = {}
    for i in range(len(attribute_names)):
        attr_type_dict[attribute_names[i]] = attribute_types[i]

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    dataset_attributes = updated_dataset.attributes
    attribute_list = [attribute.name for attribute in dataset_attributes.stream()]
    tamr_attribute_types = [attribute.type for attribute in dataset_attributes.stream()]

    assert len(attribute_list) == len(attribute_names)
    assert attribute_list == attribute_names

    for idx in range(len(attribute_types)):
        assert tamr_attribute_types[idx].spec().to_dict() == attribute_types[idx]
