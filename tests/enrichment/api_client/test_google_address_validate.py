"""Tests for Google Address Validation API functions."""

import pytest

from tamr_toolbox.enrichment.address_mapping import AddressValidationMapping
from tamr_toolbox.enrichment.api_client import google_address_validate
from tamr_toolbox.utils.testing import mock_api


def test_client_bad_key_format():
    # Set the env to something not starting with "AIza"
    key = "TestTestKeyTestKeyTestKeyTestKeyTestKey"

    with pytest.raises(ValueError, match="Invalid API key provided"):
        google_address_validate.get_maps_client(key)


@mock_api()
def test_client_invalid_key():
    key = "AIzaTestKeyTestKeyTestKeyTestKeyTestKey"
    client = google_address_validate.get_maps_client(key)

    with pytest.raises(RuntimeError, match="Invalid API key"):
        google_address_validate.validate(
            address_to_validate="66 Church St Cambridge Massachusetts 02138",
            client=client,
            region_code="US",
            enable_usps_cass=False,
        )


@mock_api()
def test_validate():
    client = google_address_validate.get_maps_client("AIzaTestKeyTestKeyTestKeyTestKeyTestKey")

    result = google_address_validate.validate(
        address_to_validate="66 Church St Cambridge Massachusetts 02138",
        client=client,
        region_code="US",
        enable_usps_cass=False,
    )

    expected = AddressValidationMapping(
        input_address="66 Church St Cambridge Massachusetts 02138",
        validated_formatted_address="66 Church Street, Cambridge, MA 02138-3733, USA",
        expiration=result.expiration,
        region_code="US",
        postal_code="02138-3733",
        admin_area="MA",
        locality="Cambridge",
        address_lines=["66 Church St"],
        usps_first_address_line="66 CHURCH ST",
        usps_city_state_zip_line="CAMBRIDGE MA 02138-3733",
        usps_city="CAMBRIDGE",
        usps_state="MA",
        usps_zip_code="02138-3733",
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

    assert result == expected


@mock_api()
def test_with_cass_and_locality():
    client = google_address_validate.get_maps_client("AIzaTestKeyTestKeyTestKeyTestKeyTestKey")

    result = google_address_validate.validate(
        address_to_validate="66 Church St Cambridge Massachusetts 02138",
        client=client,
        region_code="US",
        locality="Cambridge",
        enable_usps_cass=True,
    )

    expected = AddressValidationMapping(
        input_address="66 Church St Cambridge Massachusetts 02138",
        validated_formatted_address="66 Church Street, Cambridge, MA 02138-3733, USA",
        expiration=result.expiration,
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

    assert result == expected


@mock_api()
def test_no_result():
    client = google_address_validate.get_maps_client("AIzaTestKeyTestKeyTestKeyTestKeyTestKey")

    with pytest.raises(RuntimeError, match="Got no result"):
        google_address_validate.validate(address_to_validate="", client=client, region_code="US")
