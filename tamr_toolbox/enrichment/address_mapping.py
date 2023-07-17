"""Tasks related to creating, updating, saving, and moving address validation data from Tamr"""
import copy
import json
import logging
import os
from dataclasses import asdict, dataclass, fields
from typing import Dict, List, Optional, Union

from requests.exceptions import HTTPError
from tamr_unify_client.dataset.collection import DatasetCollection
from tamr_unify_client.dataset.resource import Dataset
from typing_extensions import Literal

from tamr_toolbox.enrichment.enrichment_utils import create_empty_mapping

LOGGER = logging.getLogger(__name__)


Granularity = Literal[
    "GRANULARITY_UNSPECIFIED",
    "SUB_PREMISE",
    "PREMISE",
    "PREMISE_PROMXIMITY",
    "BLOCK",
    "ROUTE",
    "OTHER",
]


@dataclass
class AddressValidationMapping:
    """DataClass for address validation data.

    Args:
        input_address: input address
        validated_formatted_address: the "formattedAddress" returns by the validation API, if any
        expiration: the expiration timestamp of the data, 30 days from API call
        region_code: region code returned by the validation API
        postal_code: postal code returned by the validation API
        admin_area: administrative area returned by the validation API (state for US addresses)
        locality: locality returned by the validation API (city/town for US addresses)
        address_lines: address lines returned by the validation API (e.g. ['66 Church St'])
        usps_firstAddressLine: first address line in validated USPS format, if available
        usps_cityStateZipAddressLine: : second address line in validated USPS format, if available
        usps_city: city in validated USPS format, if available
        usps_state: state in validated USPS format, if available
        usps_zipCode: str = "
        latitude: latitude associated with validated address, if any
        longitude: longitude associated with validated address, if any
        place_id: the google placeId -- the only result field not subject to the `expiration`
        input_granularity: granularity of input given by validation API
        validation_granularity: granularity of validation given by validation API
        geocode_granularity: granularity of geocode given by validation API
        has_inferred: whether the result has inferred components
        has_unconfirmed: whether the result has unconfirmed components
        has_replaced: whether the result has replaced components
        address_complete: whether the input was complete
    """

    input_address: str
    validated_formatted_address: Optional[str]
    expiration: str  # timestamp in the format given by `str(datetime.now())`
    region_code: Optional[str]
    postal_code: Optional[str]
    admin_area: Optional[str]
    locality: Optional[str]
    address_lines: List[str]
    usps_first_address_line: Optional[str]
    usps_city_state_zip_line: Optional[str]
    usps_city: Optional[str]
    usps_state: Optional[str]
    usps_zip_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    place_id: Optional[str]
    input_granularity: Granularity
    validation_granularity: Granularity
    geocode_granularity: Granularity
    has_inferred: bool
    has_unconfirmed: bool
    has_replaced: bool
    address_complete: bool


def to_dict(
    dictionary: Dict[str, AddressValidationMapping]
) -> List[Dict[str, Union[str, List[str], float, None]]]:
    """
    Convert a toolbox address validation mapping entries to  list-of-dictionary format.

    Args:
        dictionary: a toolbox address validation mapping

    Returns:
        A list of toolbox address validation mapping entries in dictionary format
    """
    return [asdict(t) for t in dictionary.values()]


def update(
    main_dictionary: Dict[str, AddressValidationMapping],
    tmp_dictionary: Dict[str, AddressValidationMapping],
) -> None:
    """
    Update a toolbox address validation mapping with another temporary address validation mapping

    Args:
        main_dictionary: the main toolbox address validation mapping containing prior results
        tmp_dictionary: a temporary toolbox address validation mapping containing new data
    """
    for input_addr, mapping in tmp_dictionary.items():
        main_dictionary[input_addr] = copy.copy(mapping)


def from_dataset(dataset: Dataset) -> Dict[str, AddressValidationMapping]:
    """
    Stream an address validation mapping dataset from Tamr.

    Args:
        dataset: Tamr Dataset object

    Returns:
        A toolbox address validation mapping

    Raises:
        ValueError: if the provided `dataset` is not a toolbox address validation mapping dataset
        NameError: if the provided `dataset` does not contain all the attributes of a
            toolbox address validation mapping
        RuntimeError: if there is any other problem while reading the `dataset` as a
            toolbox address validation mapping
    """
    if dataset.key_attribute_names[0] != "input_address":
        error_message = "Provided Tamr Dataset is not a toolbox address validation mapping"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    dictionary = {}
    for record in dataset.records():
        try:
            # Values are returned as a length-1 list of string, we change this to strings
            entry = AddressValidationMapping(
                input_address=record["input_address"],
                validated_formatted_address=record["validated_formatted_address"][0],
                expiration=record["expiration"][0],
                region_code=record["region_code"][0] if record["region_code"] else None,
                postal_code=record["postal_code"][0] if record["postal_code"] else None,
                admin_area=record["admin_area"][0] if record["admin_area"] else None,
                locality=record["locality"][0] if record["locality"] else None,
                address_lines=record["address_lines"] if record["address_lines"] else [],
                usps_first_address_line=record["usps_first_address_line"]
                if record["usps_first_address_line"]
                else None,
                usps_city_state_zip_line=record["usps_city_state_zip_line"]
                if record["usps_city_state_zip_line"]
                else None,
                usps_city=record["usps_city"] if record["usps_city"] else None,
                usps_state=record["usps_state"] if record["usps_state"] else None,
                usps_zip_code=record["usps_zip_code"] if record["usps_zip_code"] else None,
                latitude=float(record["latitude"][0]) if record["latitude"] else None,
                longitude=float(record["longitude"][0]) if record["longitude"] else None,
                place_id=record["place_id"][0] if record["place_id"] else None,
                input_granularity=record["input_granularity"][0]
                if record["input_granularity"]
                else "GRANULARITY_UNSPECIFIED",
                validation_granularity=record["validation_granularity"][0]
                if record["validation_granularity"]
                else "GRANULARITY_UNSPECIFIED",
                geocode_granularity=record["geocode_granularity"][0]
                if record["geocode_granularity"]
                else "GRANULARITY_UNSPECIFIED",
                has_inferred=record["has_inferred"][0] if record["has_inferred"] else False,
                has_unconfirmed=record["has_unconfirmed"][0]
                if record["has_unconfirmed"]
                else False,
                has_replaced=record["has_replaced"][0] if record["has_replaced"] else False,
                address_complete=record["address_complete"][0]
                if record["address_complete"]
                else False,
            )

        except KeyError as exp:
            error_message = (
                f"Supplied Tamr dataset is not in toolbox address validation mapping format: {exp}"
            )
            LOGGER.error(error_message)
            raise NameError(error_message) from exp
        except Exception as exp:
            error_message = f"Error while reading Tamr dataset address validation mapping: {exp}"
            LOGGER.error(error_message)
            raise RuntimeError(error_message) from exp

        dictionary.update({entry.input_address: entry})
    return dictionary


def to_dataset(
    addr_mapping: Dict[str, AddressValidationMapping],
    *,
    dataset: Optional[Dataset] = None,
    datasets_collection: Optional[DatasetCollection] = None,
    create_dataset: bool = False,
    dataset_name: str = "address_validation_mapping",
) -> str:
    """Ingest a toolbox address validation mapping in Tamr, creating the source dataset if needed.

    Args:
        addr_mapping: a toolbox address validation mapping
        dataset: a Tamr client dataset
        datasets_collection: a Tamr client datasets collection
        create_dataset: flag to create or upsert to an existing address validation mapping
            source dataset
        dataset_name: name to use if creating new dataset

    Returns:
        The name of the created or updated Tamr Dataset

    Raises:
        ValueError: if `create_dataset` is False and `dataset` is not provided or is not a
            toolbox address validation mapping dataset.
            If `create_dataset` is True but `datasets_collection` or `target_language` or
            `source_language` is missing or the Tamr dataset already exists
        RuntimeError: if there is an error during the creation of the Tamr dataset attributes
    """
    if create_dataset is False:
        if dataset is None:
            error_message = (
                "Tamr Client Dataset missing from inputs. Please provide a Tamr "
                "Client Dataset if updating an existing address validation dataset"
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        if dataset.key_attribute_names[0] != "input_address":
            error_message = "Provided Tamr Dataset is not a toolbox address validation mapping"
            LOGGER.error(error_message)
            raise ValueError(error_message)

    else:
        if not datasets_collection:
            error_message = (
                "Tamr Datasets Collection must be specified to create address validation dataset."
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        if dataset_name in [d.name for d in datasets_collection]:
            error_message = (
                f"Tamr Dataset {dataset_name} already exists on Tamr, you cannot duplicate it."
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        LOGGER.info("Creating toolbox address validation dataset %s on Tamr", dataset_name)
        creation_spec = {"name": dataset_name, "keyAttributeNames": ["input_address"]}
        dataset = datasets_collection.create(creation_spec)

        attributes = dataset.attributes
        for attribute in [att.name for att in fields(AddressValidationMapping)]:
            if attribute == "input_address":
                continue

            attr_spec = {
                "name": attribute,
                "type": {"baseType": "ARRAY", "innerType": {"baseType": "STRING"}},
            }
            try:
                attributes.create(attr_spec)
            except HTTPError as exp:
                error_message = (
                    f"Error while creating attribute {attribute} for dataset {dataset_name}: {exp}"
                )
                LOGGER.error(error_message)
                raise RuntimeError(error_message) from exp

    LOGGER.info("Ingesting toolbox address validation mapping to Tamr")
    dataset.upsert_records(records=to_dict(addr_mapping), primary_key_name="input_address")
    return dataset.name


def to_json(dictionary: Dict[str, AddressValidationMapping]) -> List[str]:
    """
    Convert a toolbox address validation mapping entries to a json format where set object are
    converted to list

    Args:
        dictionary: a toolbox address validation mapping

    Returns:
        A list of toolbox address validation mapping entries in json format
    """
    return [json.dumps(asdict(t)) for t in dictionary.values()]


def save(
    addr_mapping: Dict[str, AddressValidationMapping],
    addr_folder: str,
    filename: str = "address_validation_mapping.json",
) -> None:
    """
    Save a toolbox address validation mapping to disk

    Args:
        addr_mapping: dictionary object to be saved to disk
        addr_folder: base directory where mapping is saved
        filename: filename to use to save
    """
    addr_filepath = os.path.join(addr_folder, filename)

    if len(addr_mapping) > 0:
        LOGGER.debug("Writing address mapping to file")
        with open(addr_filepath, "w") as f:
            f.write("\n".join(to_json(addr_mapping)))


def load(
    addr_folder: str, filename: str = "address_validation_mapping.json"
) -> Dict[str, AddressValidationMapping]:
    """
    Load a toolbox address validation mapping from disk to memory

    Args:
        addr_folder: base directory where mapping is saved
        filename: filename where mapping is saved

    Returns:
        A toolbox address validation mapping

    Raises:
        RuntimeError: if the file was found on disk but is not of a valid toolbox address
          validation mapping type
    """
    filepath = os.path.join(addr_folder, filename)

    if not os.path.exists(filepath):
        LOGGER.info("Dictionary %s does not exist, creating an empty one.", filepath)
        filepath = create_empty_mapping(path=filepath)
        return {}

    with open(filepath, "r") as f:
        mapping_lst = [json.loads(line) for line in f.readlines()]
        try:
            # Tranform the loaded dictionaries into a AddressValidationMapping
            mapping_lst = [AddressValidationMapping(**t) for t in mapping_lst if t]
            # Make the standardized phrase the main key of the address validation mapping
            mapping_dict = {t.input_address: t for t in mapping_lst}
        except Exception as excp:
            error_message = (
                f"Could not read address validation mapping at {filepath}. "
                f"Check that the dictionary is of the correct type. Error: {excp}"
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message) from excp

    return mapping_dict
