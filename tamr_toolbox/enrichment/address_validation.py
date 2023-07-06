from datetime import datetime, timedelta
import math
import os
import json
from typing import Dict, List, Optional
import googlemaps
import logging
import pandas as pd

from tamr_toolbox import dataset
from tamr_toolbox.enrichment.address_mapping import AddressValidationMapping

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    from googlemaps import Client as GoogleMapsClient

LOGGER = logging.getLogger(__name__)


_ADDRESSVALIDATION_BASE_URL = "https://addressvalidation.googleapis.com"


def get_maps_client() -> googlemaps.Client:
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

    return googlemaps.Client(key=key)


def validate_addresses(
    dataset,
    first_line_cols_to_join: List[str],
    secnd_line_cols_to_join: List[str],
    region_code: Optional[str] = "US",
    enableUspsCass: bool = False,
    locality_col: Optional[str] = None,
) -> List[str]:
    first_lines: List[str] = get_address_strings_from_cols(dataset, first_line_cols_to_join)
    secnd_lines: List[str] = get_address_strings_from_cols(dataset, secnd_line_cols_to_join)

    address_tuples = zip(first_lines, secnd_lines)

    results = []
    for index, tup in enumerate(address_tuples):
        key = ", ".join(tup)
        if key in existing_validation_data:
            results.append(existing_validation_data[key])
            continue

        locality = df.iloc[ind][locality_col] if locality_col else None
        results.append(
            validate_address(
                addr_tuple=tup,
                locality=locality,
                region_code=region_code,
                enable_usps_cass=enable_usps_cass,
            )
        )

    return results


def get_joined_strings_from_cols(df: pd.Dataframe, cols_to_join: List[str]) -> List[str]:
    """Join specified columns in dataframe to produce single string per row of the input dataset.

    The `columns_to_join` should be supplied in the desired final format.

    Args:
        df: dataframe
        cols_to_join: the columns to be joined

    Returns:
        list of strings generated from the input dataframe
    """
    # Confirm columns specified are in dataframe
    cols_not_in_df = set(cols_to_join) - set(df.columns)
    if cols_not_in_df:
        error_message = f"Columns {cols_not_in_df} are not in supplied df: {df.columns}."
        LOGGER.error(error_message)
        raise ValueError(error_message)

    return list(df[cols_to_join].astype(str).apply(lambda x: ", ".join(x.str.strip()), axis=1))


def validate_address(
    client: googlemaps.Client,
    address_tuple: Tuple[str, str],
    region_code: Optional[str],
    locality: Optional[str],
    enable_usps_cass: bool,
) -> GoogleMapsAddressValidationResult:
    """The Google Maps Address Validation API returns a verification of an address.


    See https://developers.google.com/maps/documentation/address-validation/overview.

    Args:
        client: a client connected to the Google API
        address_tuple: address lines to validate
        region_code: the country code, e.g. 'US', 'PR', 'AU'
        locality: if supplied, results are restricted to a locality, e.g. Mountain View or AZ
        enable_usps_cass: For the "US" and "PR" regions, one may enable the Coding
            Accuracy Support System (CASS) from the United States Postal Service (USPS)
    """

    params = {"address": {"addressLines": list(address_tuple)}}

    if region_code:
        params["address"]["regionCode"] = region_code

    if locality:
        params["address"]["locality"] = locality

    if enable_usps_cass:
        params["enableUspsCass"] = True

    return client._request(
        "/v1:validateAddress",
        {},  # No GET params
        base_url=_ADDRESSVALIDATION_BASE_URL,
        extract_body=lambda x: x.json(),
        post_json=params,
    )


###################
####
###############


def clean_addresses(orig_addr: List[Optional[str]]) -> List[str]:
    """Standardize addresses to avoid re-querying previously queried addresses.

    Does basic format cleaning -- standardizing case and spacing, removing '.' and '#'.

    Args:
        orig_addr: List of addresses to standardize

    Returns:
        List of standardized text
    """
    standardized = [
        " ".join(
            (phrase if phrase is not None else "")
            .lower()
            .replace("#", "")
            .replace(".", "")
            .split()
        )
        for phrase in orig_addr
    ]
    return standardized


def get_addr_to_validate(
    orig_addr: List[str],
    addr_mapping: Dict[str, AddressValidationMapping],
    expiration_date_buffer: timedelta = timedelta(days=1),
) -> List[str]:
    """
    Find addresses not previously validated and initiate dictionary entry

    Args:
        orig_addr: list of addresses to translate
        addr_mapping: an address validation mapping
        expiration_date_buffer: re-validate addresses if they are within this period of expiring

    Returns:
        List of standardized addresses not present as keys of the mapping dictionary

    """
    count_needing_validation = 0

    for original, standard in zip(orig_addr, clean_addresses(orig_addr)):
        if standard in addr_mapping.keys():
            addr_mapping[standard].original_addresses.add(original)
        else:
            addr_mapping[standard] = AddressValidationMapping(
                original_input_addresses={original},
                input_formatted_address=standard,
                validated_formatted_address=None,
                expiration="",
                confidence=0.0,
            )
            count_needing_validation += 1

    LOGGER.info(
        "From the %s sent for validation, "
        "%s can be validated with the dictionary and "
        "%s need to be validated",
        len(orig_addr),
        len(orig_addr) - count_needing_validation,
        count_needing_validation,
    )

    to_validate = [
        t.formatted_input_address
        for t in addr_mapping.values()
        if t.validated_formatted_address is None
        or t.expiration < str(datetime.now() + expiration_date_buffer)
    ]
    LOGGER.debug("Items to validate: %s", to_validate)
    return to_validate


def from_list(
    all_addresses: List[str],
    client: "GoogleMapsClient",
    dictionary: Dict[str, AddressValidationMapping],
    *,
    chunk_size: int = 100,
    intermediate_save_every_n_chunks: Optional[int] = None,
    intermediate_save_to_disk: bool = False,
    intermediate_folder: str = "/tmp",
) -> Dict[str, AddressValidationMapping]:
    """
    Validate a list of addresses.

    The validation is saved in a dictionary on your local file system before updating the
    main dictionary.

    Args:
        all_addresses: List of standardized addresses to translate.
        client: a googlemaps api client
        dictionary: a toolbox validation dictionary
        chunk_size: number of addresses to translate per api_client calls, set too high and you
            will hit API user rate limit errors
        intermediate_save_every_n_chunks: save periodically api_client dictionary to disk every n
            chunk of addresses validated
        intermediate_save_to_disk: decide whether to save periodically the dictionary to disk to
            avoid loss of validation data if code breaks
        intermediate_folder: path to folder where dictionary will be save periodically to avoid
            loss of validation data

    Returns:
        The updated validation dictionary

    Raises:
        ValueError: if the argument chunk_size is set to 0
    """
    if chunk_size == 0:
        error_message = "validation chunk size cannot be of size 0"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    if intermediate_save_every_n_chunks == 0 or intermediate_save_every_n_chunks is None:
        intermediate_save_every_n_chunks = math.inf

    unique_all_addresses = list(set(all_addresses))
    nbr_of_unique_addresses = len(unique_all_addresses)

    addresses_to_translate = get_addr_to_validate(unique_all_addresses, dictionary)
    number_addresses_to_translate = len(addresses_to_translate)

    if number_addresses_to_translate == 0:
        LOGGER.info("All addresses to translate are found in the local dictionary.")

    else:
        LOGGER.info(
            f"Of the {nbr_of_unique_addresses} unique addresses to translate, "
            f"{number_addresses_to_translate} were not found in the dictionary."
        )

        # Google has validation rate limits
        # to avoid hitting those the addresses are sent for validation in chunks
        number_of_chunks = math.ceil(number_addresses_to_translate / chunk_size)

        tmp_dictionary = {}
        for ix, chunk_of_addresses in enumerate(_yield_chunk(addresses_to_translate, chunk_size)):
            LOGGER.debug(f"Translating chunk {ix + 1} out of {number_of_chunks}.")
            validated_addresses = google.translate(
                addresses_to_translate=chunk_of_addresses,
                client=client,
                source_language=source_language,
                target_language=target_language,
                validation_model=validation_model,
            )
            if validated_addresses is not None:
                tmp_dictionary.update(validated_addresses)

            if (ix % intermediate_save_every_n_chunks) == 0:
                LOGGER.info("Saving intermediate outputs")
                update(main_dictionary=dictionary, tmp_dictionary=tmp_dictionary)
                if intermediate_save_to_disk:
                    save(
                        validation_dictionary=dictionary,
                        dictionary_folder=intermediate_folder,
                        target_language=target_language,
                        source_language=source_language,
                    )
                # resetting temporary results after saving it
                tmp_dictionary = {}

        # update dictionary
        update(main_dictionary=dictionary, tmp_dictionary=tmp_dictionary)

    return dictionary
