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


def get_maps_client() -> "googlemaps.Client":
    """Get GoogleMaps client using the environment variable 'GOOGLEMAPS_API_KEY'

    Returns:
      API client linekd to specified key

    Raises:
      RuntimeError: if environment variable is not set
    """
    key = os.getenv("GOOGLEMAPS_API_KEY")

    if not key:
        error_message = "Environment variable GOOGLEMAPS_API_KEY is not set."
        LOGGER.error(error_message)
        raise RuntimeError(error_message)

    import googlemaps

    return googlemaps.Client(key=key)


def validate(
    *,
    address_to_validate: str,
    client: "googlemaps.Client",
    locality: Optional[str] = None,
    region_code: Optional[str] = None,
    enable_usps_cass: bool = False,
) -> Optional[AddressValidationMapping]:
    """Validate an address using google's address validation API.

    Args:
        address_to_validate:  address to validate
        client: client for Google Maps API
        region_code: region to use for validation, optional
        locality: locality to use for validation, optional
        enable_usps_cass: whether to use USPS validation

    Returns:
        toolbox address validation mapping, or None if API call fails

    Raises:
        RuntimeError: if API key is invalid
    """
    params = {"address": {"addressLines": [address_to_validate]}}

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
        raise RuntimeError(message)

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
