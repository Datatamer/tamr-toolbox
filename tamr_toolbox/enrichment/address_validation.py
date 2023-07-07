import logging
import math
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from tamr_toolbox.enrichment.address_mapping import AddressValidationMapping, save, update
from tamr_toolbox.enrichment.api_client.google_address_validate import validate

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    from googlemaps import Client as GoogleMapsClient

LOGGER = logging.getLogger(__name__)


def get_addr_to_validate(
    input_addresses: List[Tuple[Optional[str], ...]],
    addr_mapping: Dict[str, AddressValidationMapping],
    expiration_date_buffer: timedelta = timedelta(days=1),
) -> List[str]:
    """Find addresses not previously validated or validated too long ago and validate them.

    Args:
        input_addresses: list of addresses to validate
        addr_mapping: a dict of address validation mapping data
        expiration_date_buffer: re-validate addresses if they are within this period of expiring

    Returns:
        List of standardized addresses not present as keys of the mapping dictionary

    Raises:
        ValueError: if negative `expiration_date_buffer` is supplied
    """

    if str(expiration_date_buffer)[0] == "-":
        raise ValueError("Buffer time for expiration date cannot be negative.")

    count_new_addr = 0
    addr_to_validate = []

    for addr in input_addresses:
        joined_addr = " ".join([x.strip() for x in addr if x is not None])
        if joined_addr not in addr_mapping.keys():
            addr_to_validate.append(joined_addr)
            count_new_addr += 1

    LOGGER.info(
        "From %s sent for validation, %s have been previously validated; %s need validation.",
        len(input_addresses),
        len(input_addresses) - count_new_addr,
        count_new_addr,
    )

    addr_to_validate += [
        k
        for k, v in addr_mapping.items()
        if v.expiration < str(datetime.now() + expiration_date_buffer)
    ]
    LOGGER.info("Also, %s addresses need to be refreshed.", len(addr_to_validate) - count_new_addr)
    LOGGER.debug("Items to validate: %s", addr_to_validate)
    return addr_to_validate


def from_list(
    all_addresses: List[Tuple[Optional[str], ...]],
    client: "GoogleMapsClient",
    dictionary: Dict[str, AddressValidationMapping],
    *,
    region_code: Optional[str],
    enable_usps_cass: bool = False,
    intermediate_save_every_n: Optional[int] = None,
    intermediate_save_to_disk: bool = False,
    intermediate_folder: str = "/tmp",
) -> Dict[str, AddressValidationMapping]:
    """Validate a list of addresses.

    The validation is saved in a dictionary on your local file system before updating the
    main dictionary.

    Args:
        all_addresses: List of standardized addresses to validate.
        client: a googlemaps api client
        dictionary: a toolbox validation dictionary
        region_code: optional region code, e.g. 'US' or 'FR', to pass to the maps API
        enable_usps_cass: bool: whether to use USPS validation; only for 'US'/'PR' regions
        intermediate_save_every_n: save periodically api_client dictionary to disk every n
            addresses validated
        intermediate_save_to_disk: decide whether to save periodically the dictionary to disk to
            avoid loss of validation data if code breaks
        intermediate_folder: path to folder where dictionary will be save periodically to avoid
            loss of validation data

    Returns:
        The updated validation dictionary
    """
    if intermediate_save_every_n == 0 or intermediate_save_every_n is None:
        intermediate_save_every_n = math.inf

    unique_all_addresses = list(set(all_addresses))
    nbr_of_unique_addresses = len(unique_all_addresses)

    addresses_to_validate = get_addr_to_validate(unique_all_addresses, dictionary)
    nbr_addresses_to_validate = len(addresses_to_validate)

    if nbr_addresses_to_validate == 0:
        LOGGER.info("All addresses to validate are found in the local dictionary.")

    else:
        LOGGER.info(
            "Of %s addresses to validate, %s were not found in the dictionary or were too old.",
            nbr_of_unique_addresses,
            nbr_addresses_to_validate,
        )

        tmp_dictionary = {}
        for idx, address in enumerate(addresses_to_validate):
            validated_address = validate(
                address_to_validate=address,
                client=client,
                locality=None,  # TODO: decide how to pass this through
                region_code=region_code,
                enable_usps_cass=enable_usps_cass,
            )
            if validated_address is not None:
                tmp_dictionary.update({address: validated_address})

            if (idx % intermediate_save_every_n) == 0:
                LOGGER.info("Saving intermediate outputs")
                update(main_dictionary=dictionary, tmp_dictionary=tmp_dictionary)
                if intermediate_save_to_disk:
                    save(addr_mapping=dictionary, addr_folder=intermediate_folder)
                # Reset temporary results after saving
                tmp_dictionary = {}

        # update dictionary
        update(main_dictionary=dictionary, tmp_dictionary=tmp_dictionary)
        if intermediate_save_to_disk:
            save(addr_mapping=dictionary, addr_folder=intermediate_folder)

    return dictionary
