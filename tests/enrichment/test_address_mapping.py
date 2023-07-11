"""Tests for class and functions related to AddressValidationMapping."""

from unittest.mock import patch

import pytest

import tamr_toolbox.enrichment.address_mapping as address_mapping
from tamr_toolbox import enrichment, utils
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

VALIDATION_MAPPING_DATASET_ID = "1147"


def test_to_dict():
    mapping = {
        "test": address_mapping.AddressValidationMapping(
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
    }
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


def test_mapping_update():
    mapping0 = {
        "test": address_mapping.AddressValidationMapping(
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
    }

    mapping1 = {
        "another_test": address_mapping.AddressValidationMapping(
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
    }

    address_mapping.update(mapping0, mapping1)

    assert set(mapping0.keys()) == {"test", "another_test"}
    assert mapping0["another_test"] == mapping1["another_test"]


@mock_api(enforce_online_test=True)
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
