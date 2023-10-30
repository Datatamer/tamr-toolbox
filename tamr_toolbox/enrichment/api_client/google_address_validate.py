"""Functions to interact with Google Maps API Client."""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from tamr_toolbox.enrichment.address_mapping import AddressValidationMapping
from tamr_toolbox.models.data_type import JsonDict

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    import googlemaps


LOGGER = logging.getLogger(__name__)
_ADDRESSVALIDATION_BASE_URL = "https://addressvalidation.googleapis.com"


def get_maps_client(googlemaps_api_key: str) -> "googlemaps.Client":
    """Get GoogleMaps client.

    Args:
        googlemaps_api_key: API key for the Google Maps address validation API

    Returns:
        API client linekd to specified key
    """
    import googlemaps

    return googlemaps.Client(key=googlemaps_api_key)


def validate(
    *,
    address_to_validate: str,
    client: "googlemaps.Client",
    locality: Optional[str] = None,
    region_code: Optional[str] = None,
    enable_usps_cass: bool = False,
    fail_on_api_error: bool = False,
) -> AddressValidationMapping:
    """Validate an address using google's address validation API.

    Args:
        address_to_validate:  address to validate
        client: client for Google Maps API
        region_code: region to use for validation, optional
        locality: locality to use for validation, optional
        enable_usps_cass: whether to use USPS validation
        fail_on_api_error: whether to raise an error if API call fails, or continue processing

    Returns:
        toolbox address validation mapping

    Raises:
        RuntimeError: if API key is invalid, or if API call fails and fail_on_api_error is True
    """

    if address_to_validate == "":
        return get_empty_address_validation("")

    params: JsonDict = {"address": {"addressLines": [address_to_validate]}}

    if region_code:
        params["address"]["regionCode"] = region_code

    if locality:
        params["address"]["locality"] = locality

    if enable_usps_cass:
        params["enableUspsCass"] = True

    json_resp: JsonDict = client._request(
        "/v1:validateAddress",
        {},  # No GET params
        base_url=_ADDRESSVALIDATION_BASE_URL,
        extract_body=lambda x: x.json(),
        post_json=params,
    )

    if "api key not valid" in json_resp.get("error", dict()).get("message", "").lower():
        message = "Invalid API key supplied to Google Maps address validation"
        LOGGER.error(message)
        raise RuntimeError(message)

    if not json_resp.get("result"):
        message = f"Got no result for {address_to_validate}: API returned {json_resp}."
        LOGGER.error(message)
        if fail_on_api_error:
            raise RuntimeError(message)
        else:
            return get_empty_address_validation(address_to_validate)

    # Parse the response to extract the desired fields
    json_resp = json_resp["result"]

    # Get USPS address components
    usps_address: JsonDict = json_resp.get("uspsData", dict()).get("standardizedAddress", dict())
    usps_zipcode = usps_address.get("zipCode")
    if usps_zipcode and usps_address.get("zipCodeExtension"):
        usps_zipcode += "-" + usps_address.get("zipCodeExtension")

    # Get postal address components
    postal_address = json_resp.get("address", dict()).get("postalAddress", dict())

    return AddressValidationMapping(
        input_address=address_to_validate,
        validated_formatted_address=json_resp["address"]["formattedAddress"],
        expiration=str(datetime.now() + timedelta(days=30)),
        region_code=postal_address.get("regionCode"),
        postal_code=postal_address.get("postalCode"),
        admin_area=postal_address.get("administrativeArea"),
        locality=postal_address.get("locality"),
        address_lines=postal_address.get("addressLines"),
        usps_first_address_line=usps_address.get("firstAddressLine"),
        usps_city_state_zip_line=usps_address.get("cityStateZipAddressLine"),
        usps_city=usps_address.get("city"),
        usps_state=usps_address.get("state"),
        usps_zip_code=usps_zipcode,
        latitude=json_resp.get("geocode", dict()).get("location", dict()).get("latitude"),
        longitude=json_resp.get("geocode", dict()).get("location", dict()).get("longitude"),
        place_id=json_resp.get("geocode", dict()).get("placeId"),
        input_granularity=json_resp.get("verdict", dict()).get(
            "inputGranularity", "GRANULARITY_UNSPECIFIED"
        ),
        validation_granularity=json_resp.get("verdict", dict()).get(
            "validationGranularity", "GRANULARITY_UNSPECIFIED"
        ),
        geocode_granularity=json_resp.get("verdict", dict()).get(
            "geocodeGranularity", "GRANULARITY_UNSPECIFIED"
        ),
        has_inferred=json_resp.get("verdict", dict()).get("hasInferredComponents", False),
        has_unconfirmed=json_resp.get("verdict", dict()).get("hasUnconfirmedComponents", False),
        has_replaced=json_resp.get("verdict", dict()).get("hasReplacedComponents", False),
        address_complete=json_resp.get("verdict", dict()).get("addressComplete", False),
    )


def get_empty_address_validation(input_addr: str) -> AddressValidationMapping:
    """Get address validation data with only input address; other fields set to empty or default
    values.

    Expiration date is set to the year 2100 to avoid excess called to the API with null or bad
    data.

    Args:
        input_address: the address to be validated

    Returns:
        A validation object with all fields except `input_address` and `expiration` set to null
        values.
    """
    return AddressValidationMapping(
        input_address=input_addr,
        validated_formatted_address=None,
        expiration=str(datetime(2100, 1, 1)),
        region_code=None,
        postal_code=None,
        admin_area=None,
        locality=None,
        address_lines=[],
        usps_first_address_line=None,
        usps_city_state_zip_line=None,
        usps_city=None,
        usps_state=None,
        usps_zip_code=None,
        latitude=None,
        longitude=None,
        place_id=None,
        input_granularity="GRANULARITY_UNSPECIFIED",
        validation_granularity="GRANULARITY_UNSPECIFIED",
        geocode_granularity="GRANULARITY_UNSPECIFIED",
        has_inferred=True,
        has_unconfirmed=True,
        has_replaced=True,
        address_complete=False,
    )
