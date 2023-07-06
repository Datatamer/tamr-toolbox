"""Tasks related to creating, updating, saving, and moving address validation data from Tamr"""
import copy
import json
import logging
from dataclasses import asdict, dataclass, fields
import os
from typing import Dict, List, Optional, Set, Union

from requests.exceptions import HTTPError
from tamr_unify_client.dataset.collection import DatasetCollection
from tamr_unify_client.dataset.resource import Dataset

from tamr_toolbox.enrichment.enrichment_utils import SetEncoder

LOGGER = logging.getLogger(__name__)


@dataclass
class AddressValidationMapping:
    """
    DataClass for address validation data.

    Args:
        input_formatted_address: input address with basic cleaning, formatting applied
        original_input_addresses: set of addresses corresponding to same formatted input address
        validated_formatted_address: the "formattedAddress" returns by the validation API, if any
        expiration: the expiration timestamp of the data, 30 days from API call
        confidence: an indicator of the confidence of the validation
        region: the region code used in the API call
        latitude: latitude associated with validated address, if any
        longitude: longitude associated with validated address, if any
        placeId: the google placeId -- the only result field not subject to the `expiration`
    """

    input_formatted_address: str
    original_input_addresses: Set[str]
    validated_formatted_address: Optional[str]
    expiration: str
    confidence: float
    region: str = "US"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    place_id: Optional[str] = None


def to_dict(dictionary: Dict[str, AddressValidationMapping]) -> List[Dict[str, Union[str, List]]]:
    """
    Convert a toolbox address validation mapping entries to a dictionary format.

    Set object are converted to lists.

    Args:
        dictionary: a toolbox address validation mapping

    Returns:
        A list of toolbox address validation mapping entries in dictionary format
    """
    return [json.loads(json.dumps(asdict(t), cls=SetEncoder)) for t in dictionary.values()]


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
    for formatted_input_addr, mapping in tmp_dictionary.items():
        if formatted_input_addr in main_dictionary:
            existing = main_dictionary[formatted_input_addr]
            for att in [f.name for f in fields(AddressValidationMapping)]:
                if att == "original_input_addresses":
                    existing.original_input_addresses = existing.original_input_addresses.union(
                        mapping.original_input_addresses
                    )
                else:
                    setattr(existing, att, getattr(mapping.attr))

        else:
            main_dictionary[formatted_input_addr] = copy.copy(mapping)


def convert_to_mappings(dictionary: Dict[str, AddressValidationMapping]) -> Dict[str, str]:
    """
    Transform a address validation mapping into a mapping of original addresses to standardized.

    Args:
        dictionary: a toolbox address validation mapping

    Returns:
        a dictionary with original address as key and validated/standardized address as value
    """
    mapping_from_dictionary = {
        orig: t.validated_formatted_address
        for t in dictionary.values()
        for orig in t.original_input_addresses
    }
    return mapping_from_dictionary


def from_dataset(dataset: Dataset) -> Dict[str, AddressValidationMapping]:
    """
    Stream a dictionary from Tamr

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
    if dataset.key_attribute_names[0] != "validated_formatted_address":
        error_message = "Provided Tamr Dataset is not a toolbox address validation mapping"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    dictionary = {}
    for record in dataset.records():
        try:
            # Values are returned as a length-1 list of string, we change this to strings
            entry = AddressValidationMapping(
                input_formatted_address=record["input_formatted_address"][0],
                validated_formatted_address=record["validated_formatted_address"][0],
                expiration=record["expiration"][0],
                confidence=float(record["confidence"][0]),
                region=record["region"][0],
                latitude=float(record["latitude"][0]) if record["latitude"] else None,
                longitude=float(record["longitude"][0]) if record["longitude"] else None,
                place_id=record["place_id"][0] if record["place_id"] else None,
                # Original addresses are stored on Tamr as lists, we save it as a set
                original_input_addresses=set(record["original_input_addresses"]),
            )

        except NameError as exp:
            error_message = (
                f"Supplied Tamr dataset is not in toolbox address validation mapping format: {exp}"
            )
            LOGGER.error(error_message)
            raise NameError(error_message) from exp
        except Exception as exp:
            error_message = f"Error while reading Tamr dataset address validation mapping: {exp}"
            LOGGER.error(error_message)
            raise RuntimeError(error_message) from exp

        dictionary.update({entry.input_formatted_address: entry})
    return dictionary


def to_dataset(
    dictionary: Dict[str, AddressValidationMapping],
    *,
    dataset: Optional[Dataset] = None,
    datasets_collection: Optional[DatasetCollection] = None,
    create_dataset: bool = False,
) -> str:
    """
    Ingest a toolbox dictionary in Tamr, creates the source dataset if it doesn't exists

    Args:
        dictionary: a toolbox address validation mapping
        dataset: a Tamr client dataset
        datasets_collection: a Tamr client datasets collection
        create_dataset: flag to create or upsert to an existing address validation mapping
            source dataset

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

        if dataset.key_attribute_names[0] != "validated_formatted_address":
            error_message = "Provided Tamr Dataset is not a toolbox address validation mapping"
            LOGGER.error(error_message)
            raise ValueError(error_message)

    else:
        if not (datasets_collection):
            error_message = (
                "A Tamr Datasets Collection must be input if creating the toolbox "
                "address validation dataset."
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        # Get dataset name using filename function
        # The value of dictionary folder here is unimportant
        dataset_name = "address_validation_mapping"
        if dataset_name in [d.name for d in datasets_collection]:
            error_message = (
                f"Tamr Dataset {dataset_name} already exists on Tamr, you cannot "
                f"create a dataset with the same name as another one"
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        LOGGER.info("Creating toolbox address validation dataset %s on Tamr", dataset_name)
        creation_spec = {
            "name": dataset_name,
            "keyAttributeNames": ["validated_formatted_address"],
        }
        dataset = datasets_collection.create(creation_spec)

        attributes = dataset.attributes
        for attribute in [att.name for att in fields(AddressValidationMapping)]:
            if attribute == "validated_formatted_address":
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
    dataset.upsert_records(
        records=to_dict(dictionary), primary_key_name="validated_formatted_address"
    )
    return dataset.name


def create(path: str) -> str:
    """
    Create an empty mapping on disk.

    Args:
        path: location where empty mapping is created

    Returns:
        A path to the new empty file
    """
    with open(path, "w") as f:
        f.write(json.dumps({}))
    return path


def to_json(dictionary: Dict[str, AddressValidationMapping]) -> List[str]:
    """
    Convert a toolbox address validation mapping entries to a json format where set object are
    converted to list

    Args:
        dictionary: a toolbox address validation mapping

    Returns:
        A list of toolbox address validation mapping entries in json format
    """
    return [json.dumps(asdict(t), cls=SetEncoder) for t in dictionary.values()]


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
        filepath = create(path=filepath)

    with open(filepath, "r") as f:
        mapping_lst = [json.loads(line) for line in f.readlines()]
        try:
            # Tranform the loaded dictionaries into a AddressValidationMapping
            mapping_lst = [AddressValidationMapping(**t) for t in mapping_lst]
            # Change original phrases from List to Set
            for dictionary in mapping_lst:
                dictionary.original_phrases = set(dictionary.original_phrases)
            # Make the standardized phrase the main key of the address validation mapping
            # to access each translation easily
            mapping_dict = {
                t.standardized_phrase: t for t in mapping_lst if t.standardized_phrase is not None
            }
        except Exception as excp:
            error_message = (
                f"Could not read address validation mapping at {filepath}. "
                f"Check that the dictionary is of the correct type. Error: {excp}"
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message) from excp

    return mapping_dict
