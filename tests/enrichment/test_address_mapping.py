"""Tests for class and functions related to AddressValidationMapping."""

import json
import os
from dataclasses import asdict
from unittest.mock import patch

import pytest
from requests import HTTPError
from tamr_unify_client.attribute.collection import AttributeCollection
from tamr_unify_client.dataset.resource import Dataset

import tamr_toolbox
import tamr_toolbox.enrichment.address_mapping as address_mapping
from tamr_toolbox import enrichment, utils
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

DATASET_TO_BE_VALIDATED_ID = "1144"
VALIDATION_MAPPING_DATASET_ID = "1147"

ADDR_VAL_MAPPING_0 = address_mapping.AddressValidationMapping(
    input_address="66 Church St Cambridge Massachusetts 02138",
    validated_formatted_address="66 Church Street, Cambridge, MA 02138-3733, USA",
    expiration="2023-07-11 11:21:21.784829",
    region_code="US",
    postal_code="02138-3733",
    admin_area="MA",
    locality="Cambridge",
    address_lines=["66 Church St"],
    usps_first_address_line="66 CHURCH ST CAMBRIDGE MASSACHUSETTS 02138",
    usps_city_state_zip_line=None,
    usps_city=None,
    usps_state=None,
    usps_zip_code=None,
    latitude=42.3739503,
    longitude=-71.1211445,
    place_id="ChIJNR2ZIGh344kRNQAj-dh6d00",
    input_granularity="PREMISE",
    validation_granularity="PREMISE",
    geocode_granularity="PREMISE",
    has_inferred=True,
    has_unconfirmed=False,
    has_replaced=False,
    address_complete=False,
)

ADDR_VAL_MAPPING_1 = address_mapping.AddressValidationMapping(
    input_address="66 Church St Cambridge Mass 2138",
    validated_formatted_address="66 Church Street, Cambridge, MA 02138-3733, USA",
    expiration="2023-07-11 11:21:21.784829",
    region_code="US",
    postal_code="02138-3733",
    admin_area="MA",
    locality="Cambridge",
    address_lines=["66 Church St"],
    usps_first_address_line="66 CHURCH ST CAMBRIDGE MASSACHUSETTS 02138",
    usps_city_state_zip_line=None,
    usps_city=None,
    usps_state=None,
    usps_zip_code=None,
    latitude=42.3739503,
    longitude=-71.1211445,
    place_id="ChIJNR2ZIGh344kRNQAj-dh6d00",
    input_granularity="PREMISE",
    validation_granularity="PREMISE",
    geocode_granularity="PREMISE",
    has_inferred=True,
    has_unconfirmed=False,
    has_replaced=False,
    address_complete=False,
)


def test_to_dict():
    mapping = {"test": ADDR_VAL_MAPPING_0}
    result = address_mapping.to_dict(mapping)
    assert len(result) == 1

    assert result[0] == {
        "input_address": "66 Church St Cambridge Massachusetts 02138",
        "validated_formatted_address": "66 Church Street, Cambridge, MA 02138-3733, USA",
        "expiration": "2023-07-11 11:21:21.784829",
        "region_code": "US",
        "postal_code": "02138-3733",
        "admin_area": "MA",
        "locality": "Cambridge",
        "address_lines": ["66 Church St"],
        "usps_first_address_line": "66 CHURCH ST CAMBRIDGE MASSACHUSETTS 02138",
        "usps_city_state_zip_line": None,
        "usps_city": None,
        "usps_state": None,
        "usps_zip_code": None,
        "latitude": 42.3739503,
        "longitude": -71.1211445,
        "place_id": "ChIJNR2ZIGh344kRNQAj-dh6d00",
        "address_complete": False,
        "geocode_granularity": "PREMISE",
        "has_inferred": True,
        "has_replaced": False,
        "has_unconfirmed": False,
        "input_granularity": "PREMISE",
        "validation_granularity": "PREMISE",
    }


def test_mapping_update_and_json():
    mapping0 = {"test": ADDR_VAL_MAPPING_0}
    mapping1 = {"another_test": ADDR_VAL_MAPPING_1}

    address_mapping.update(mapping0, mapping1)

    assert set(mapping0.keys()) == {"test", "another_test"}
    assert mapping0["another_test"] == mapping1["another_test"]

    assert [json.loads(x) for x in address_mapping.to_json(mapping0)][0] == asdict(
        list(mapping0.values())[0]
    )


@mock_api()
def test_address_validation_mapping_from_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(VALIDATION_MAPPING_DATASET_ID)
    assert len(enrichment.address_mapping.from_dataset(dataset)) == 14

    with patch.object(address_mapping.AddressValidationMapping, "__init__", new=lambda cls: []):
        dataset = client.datasets.by_resource_id(VALIDATION_MAPPING_DATASET_ID)
        with pytest.raises(
            RuntimeError, match="Error while reading Tamr dataset address validation mapping"
        ):
            enrichment.address_mapping.from_dataset(dataset)


@mock_api()
def test_address_validation_bad_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    with pytest.raises(ValueError, match="not a toolbox address validation mapping"):
        dataset = client.datasets.by_resource_id(DATASET_TO_BE_VALIDATED_ID)
        address_mapping.from_dataset(dataset)


def test_load_save_mapping():
    dir = os.path.join(get_toolbox_root_dir(), "tests", "enrichment")
    mapping = address_mapping.load(addr_folder=dir)
    assert len(mapping) == 5

    assert not os.path.exists(os.path.join(dir, "temp.json"))
    address_mapping.save(addr_mapping=mapping, addr_folder=dir, filename="temp.json")
    mapping_from_save = address_mapping.load(addr_folder=dir, filename="temp.json")
    assert mapping_from_save == mapping
    os.remove(os.path.join(dir, "temp.json"))


def test_load_mapping_bad_format():
    dir = os.path.join(get_toolbox_root_dir(), "tests", "enrichment")

    with pytest.raises(RuntimeError, match="Could not read address validation mapping"):
        address_mapping.load(
            addr_folder=dir, filename="address_validation_mapping_bad_format.json"
        )


def test_load_dict_does_not_exist():
    dir = os.path.join(get_toolbox_root_dir(), "tests", "enrichment")
    path = os.path.join(dir, "this_file_does_not_exist.json")

    assert not os.path.exists(path)

    mapping = address_mapping.load(addr_folder=dir, filename="this_file_does_not_exist.json")
    assert mapping == {}

    assert os.path.exists(path)
    os.remove(path)


@mock_api()
def test_to_dataset_errors():
    data = {"test": ADDR_VAL_MAPPING_0, "another": ADDR_VAL_MAPPING_1}

    with pytest.raises(ValueError, match="Tamr Client Dataset missing from inputs"):
        address_mapping.to_dataset(data)

    with pytest.raises(ValueError, match="Tamr Datasets Collection must be specified"):
        address_mapping.to_dataset(data, create_dataset=True)

    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    with pytest.raises(ValueError, match="Dataset is not a toolbox address validation mapping"):
        dataset = client.datasets.by_resource_id(DATASET_TO_BE_VALIDATED_ID)
        address_mapping.to_dataset(data, dataset=dataset)

    with pytest.raises(ValueError, match=" already exists on Tamr"):
        address_mapping.to_dataset(data, datasets_collection=client.datasets, create_dataset=True)


@mock_api()
def test_to_dataset_create():
    data = {"test": ADDR_VAL_MAPPING_0, "another": ADDR_VAL_MAPPING_1}
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset_name = "temp_address_validation_mapping"

    if tamr_toolbox.dataset.manage.exists(client=client, dataset_name=dataset_name):
        dataset = client.datasets.by_name(dataset_name)
        dataset.delete()

    address_mapping.to_dataset(
        data, create_dataset=True, datasets_collection=client.datasets, dataset_name=dataset_name
    )

    assert tamr_toolbox.dataset.manage.exists(client=client, dataset_name=dataset_name)
    # Clean up
    dataset = client.datasets.by_name(dataset_name)
    dataset.delete()
    assert not tamr_toolbox.dataset.manage.exists(client=client, dataset_name=dataset_name)

    with patch.object(AttributeCollection, "create") as mock_attrib_create:
        mock_attrib_create.side_effect = HTTPError("Mock HTTP error")
        with pytest.raises(RuntimeError, match="Mock HTTP error"):
            address_mapping.to_dataset(
                data,
                create_dataset=True,
                datasets_collection=client.datasets,
                dataset_name=dataset_name,
            )
        assert mock_attrib_create.call_count == 1

    # Clean up
    if tamr_toolbox.dataset.manage.exists(client=client, dataset_name=dataset_name):
        dataset = client.datasets.by_name(dataset_name)
        dataset.delete()
    assert not tamr_toolbox.dataset.manage.exists(client=client, dataset_name=dataset_name)


@mock_api()
def test_from_dataset_bad_format():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(DATASET_TO_BE_VALIDATED_ID)

    with patch.object(Dataset, "key_attribute_names", new=["input_address"]):
        with pytest.raises(
            NameError,
            match="Supplied Tamr dataset is not in toolbox address validation mapping format",
        ):
            enrichment.address_mapping.from_dataset(dataset)
