"""Tasks related to validation and refresh of address data using Google Maps API"""

import logging
import os
from dataclasses import replace
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

import tamr_toolbox
from tamr_toolbox.enrichment.address_validation import from_list, get_addr_to_validate
from tamr_toolbox.enrichment.api_client.google_address_validate import get_maps_client
from tamr_toolbox.utils.testing import mock_api

ADDR_VAL_MAPPING_0 = tamr_toolbox.enrichment.address_mapping.AddressValidationMapping(
    input_address="66 CHURCH ST CAMBRIDGE MASS 02138",
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


def test_bad_buffer_time():
    with pytest.raises(ValueError, match="Buffer time for expiration date cannot be negative."):
        get_addr_to_validate(
            input_addresses=[], addr_mapping={}, expiration_date_buffer=timedelta(days=-1)
        )


def test_empty_addr_mapping():
    result = get_addr_to_validate(
        input_addresses=[("test", "address 1"), ("test address 2",), (None, "TEST")],
        addr_mapping={},
    )

    assert result == ["TEST ADDRESS 1", "TEST ADDRESS 2", "TEST"]


def test_stale_addr_mapping():
    with patch.object(
        logging.getLogger("tamr_toolbox.enrichment.address_validation"), "info"
    ) as mock_logger:
        result = get_addr_to_validate(
            input_addresses=[("test", "address 1"), ("test address 2",), (None, "TEST")],
            addr_mapping={"TEST ADDRESS 2": ADDR_VAL_MAPPING_0},
        )
        mock_logger.assert_called_once_with(
            "From %s sent for validation, %s have been not been validated before; %s are stale.",
            3,
            2,
            1,
        )

    assert result == ["TEST ADDRESS 1", "TEST ADDRESS 2", "TEST"]


def test_from_list_all_in_dict():
    existing_var = os.getenv("GOOGLEMAPS_API_KEY")
    os.environ["GOOGLEMAPS_API_KEY"] = "AIzaTestKeyTestKeyTestKeyTestKeyTestKey"

    with patch.object(
        logging.getLogger("tamr_toolbox.enrichment.address_validation"), "info"
    ) as mock_logger:
        from_list(
            all_addresses=[("test", "address 1")],
            client=get_maps_client(),
            dictionary={
                "TEST ADDRESS 1": replace(
                    ADDR_VAL_MAPPING_0, expiration=str(datetime.now() + timedelta(days=2))
                )
            },
            region_code="US",
        )
        mock_logger.assert_called_with(
            "All addresses to validate are found in the local dictionary."
        )

    # Reset to state before test started
    if existing_var:
        os.environ["GOOGLEMAPS_API_KEY"] = existing_var
    else:
        os.environ.pop("GOOGLEMAPS_API_KEY")


@mock_api()
def test_from_list():
    # Set to filler key for mock API
    existing_var = os.getenv("GOOGLEMAPS_API_KEY")
    os.environ["GOOGLEMAPS_API_KEY"] = "AIzaTestKeyTestKeyTestKeyTestKeyTestKey"

    result = from_list(
        all_addresses=[("66 church st", "cambridge", "mass", "02138")],
        client=get_maps_client(),
        dictionary={},
        region_code="US",
    )

    assert list(result.keys()) == ["66 CHURCH ST CAMBRIDGE MASS 02138"]
    assert (
        result["66 CHURCH ST CAMBRIDGE MASS 02138"].validated_formatted_address
        == ADDR_VAL_MAPPING_0.validated_formatted_address
    )

    # Reset to state before test started
    if existing_var:
        os.environ["GOOGLEMAPS_API_KEY"] = existing_var
    else:
        os.environ.pop("GOOGLEMAPS_API_KEY")


@mock_api()
def test_from_list_intermediate_save():
    existing_var = os.getenv("GOOGLEMAPS_API_KEY")
    os.environ["GOOGLEMAPS_API_KEY"] = "AIzaTestKeyTestKeyTestKeyTestKeyTestKey"

    # Test saving after every lookup
    with patch.object(tamr_toolbox.enrichment.address_validation, "save") as mock_save:
        result = from_list(
            all_addresses=[("66 church st", "cambridge", "mass", "02138")],
            client=get_maps_client(),
            dictionary={},
            region_code="US",
            intermediate_save_to_disk=True,
            intermediate_save_every_n=1,
        )
        mock_save.assert_called_with(addr_mapping=result, addr_folder="/tmp")

    # Test saving only at end
    with patch.object(tamr_toolbox.enrichment.address_validation, "save") as mock_save:
        result = from_list(
            all_addresses=[("66 church st", "cambridge", "mass", "02138")],
            client=get_maps_client(),
            dictionary={},
            region_code="US",
            intermediate_save_to_disk=True,
        )
        mock_save.assert_called_once_with(addr_mapping=result, addr_folder="/tmp")

    # Reset to state before test started
    if os.path.exists("/tmp/address_validation_mapping.json"):
        os.remove("/tmp/address_validation_mapping.json")
    if existing_var:
        os.environ["GOOGLEMAPS_API_KEY"] = existing_var
    else:
        os.environ.pop("GOOGLEMAPS_API_KEY")
