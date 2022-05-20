"""Tasks related to creating, updating, saving and moving translation dictionaries
in and out of Tamr"""
from typing import Dict, Set, List, Optional, Union
from dataclasses import dataclass, asdict, field
from tamr_unify_client.dataset.collection import DatasetCollection
from tamr_unify_client.dataset.resource import Dataset
from requests.exceptions import HTTPError
from pathlib import Path

import json
import os
import logging

LOGGER = logging.getLogger(__name__)


@dataclass
class TranslationDictionary:
    """
    A DataClass for translation dictionaries

    Args:
        standardized_phrase: The unique common standardized version of all original_phrases
        translated_phrase: The translated standardized phrase to the target language of the
            dictionary
        detected_language: The language detected of the standardized phrase if source lanaguage is
            set to auto
        original_phrases: A set of original phrases which all convert to the standardized phrases
            when applying standardization
    """

    standardized_phrase: str = None
    translated_phrase: str = None
    detected_language: str = None
    original_phrases: Set[str] = field(default_factory=lambda: set())


class SetEncoder(json.JSONEncoder):
    """
    A Class to transform type 'set' to type 'list' when saving objects to JSON format
    """

    def default(self, python_object):
        """
        Transform a set into a list if input is a set

        Args:
            python_object: the python object to be saved to a json format

        Returns:
            Default json encoder format of input object or List if input is a Set
        """
        if isinstance(python_object, set):
            return list(python_object)
        return json.JSONEncoder.default(self, python_object)


def filename(
    dictionary_folder: Union[str, Path],
    *,
    target_language: str = "en",
    source_language: str = "auto",
) -> str:
    """
    Generate a toolbox translation dictionary file path

    Args:
        dictionary_folder: base directory where dictionaries are saved
        target_language: the language to translate into, for a list of allowed inputs:
            https://cloud.google.com/translate/docs/basic/discovering-supported-languages
        source_language: the language the text to translate is in, if None, assumes it is "auto"

    Returns:
        A toolbox translation dictionary file path

    """
    if source_language is None:
        source_language = "auto"
    dictionary_name = f"dictionary_{source_language.lower()}_to_{target_language.lower()}.json"
    if isinstance(dictionary_folder, str):
        dictionary_folder = Path(dictionary_folder)
    return str(dictionary_folder / dictionary_name)


def create(
    dictionary_folder: str, *, target_language: str = "en", source_language: str = "auto"
) -> str:
    """
    Create an empty dictionary on disk

    Args:
        dictionary_folder: base directory where dictionary is saved
        target_language: the language to translate into, for a list of allowed inputs:
            https://cloud.google.com/translate/docs/basic/discovering-supported-languages
        source_language: the language the text to translate is in, if None, assumes it is "auto"

    Returns:
        A path to a dictionary
    """
    dictionary_filepath = filename(
        dictionary_folder, target_language=target_language, source_language=source_language
    )
    with open(dictionary_filepath, "w") as f:
        f.write(json.dumps({}))
    return dictionary_filepath


def to_json(dictionary: Dict[str, TranslationDictionary]) -> List[str]:
    """
    Convert a toolbox translation dictionary entries to a json format where set object are
    converted to list

    Args:
        dictionary: a toolbox translation dictionary

    Returns:
        A list of toolbox translation dictionary entries in json format
    """
    return [json.dumps(asdict(t), cls=SetEncoder) for t in dictionary.values()]


def to_dict(dictionary: Dict[str, TranslationDictionary]) -> List[Dict[str, Union[str, List]]]:
    """
    Convert a toolbox translation dictionary entries to a dictionary format where set object are
    converted to list

    Args:
        dictionary: a toolbox translation dictionary

    Returns:
        A list of toolbox translation dictionary entries in dictionary format
    """
    return [json.loads(json.dumps(asdict(t), cls=SetEncoder)) for t in dictionary.values()]


def save(
    translation_dictionary: Dict[str, TranslationDictionary],
    dictionary_folder: str,
    *,
    target_language: str = "en",
    source_language: str = "auto",
) -> None:
    """
    Save a toolbox translation dictionary to disk

    Args:
        translation_dictionary: dictionary object to be saved to disk
        dictionary_folder: base directory where dictionary is saved
        target_language: the language to translate into, for a list of allowed inputs:
            https://cloud.google.com/translate/docs/basic/discovering-supported-languages
        source_language: the language the text to translate is in, if None, assumes it is "auto"

    Returns:

    """
    dictionary_filepath = filename(
        dictionary_folder, target_language=target_language, source_language=source_language
    )

    if len(translation_dictionary) > 0:
        LOGGER.debug("Writing Dictionary to file")
        with open(dictionary_filepath, "w") as f:
            f.write("\n".join(to_json(translation_dictionary)))


def load(
    dictionary_folder: str, *, target_language: str = "en", source_language: str = "auto"
) -> Dict[str, TranslationDictionary]:
    """
    Load a toolbox translation dictionary from disk to memory

    Args:
        dictionary_folder: base directory where dictionary is saved
        target_language: the language to translate into, for a list of allowed inputs:
            https://cloud.google.com/translate/docs/basic/discovering-supported-languages
        source_language: the language the text to translate is in, if None, assumes it is "auto"

    Returns:
        A toolbox translation dictionary

    Raises:
        RuntimeError: if the dictionary was found on disk but is not of a valid
            toolbox translation dictionary type
    """
    dictionary_filepath = filename(
        dictionary_folder, target_language=target_language, source_language=source_language
    )
    if not os.path.exists(dictionary_filepath):
        LOGGER.info(f"Dictionary {dictionary_filepath} does not exists, creating an empty one.")
        dictionary_filepath = create(
            dictionary_folder, target_language=target_language, source_language=source_language
        )

    with open(dictionary_filepath, "r") as f:
        translation_dictionary = [json.loads(line) for line in f.readlines()]
        try:
            # Tranform the loaded dictionaries into a TranslationDictionary
            translation_dictionary = [TranslationDictionary(**t) for t in translation_dictionary]
            # Change original phrases from List to Set
            for dictionary in translation_dictionary:
                dictionary.original_phrases = set(dictionary.original_phrases)
            # Make the standardized phrase the main key of the translation dictionary to be able
            # to access each translation easily
            translation_dictionary = {
                t.standardized_phrase: t
                for t in translation_dictionary
                if t.standardized_phrase is not None
            }
        except Exception as e:
            error_message = (
                f"Could not read translation dictionary at {dictionary_filepath}. "
                f"Check that the dictionary is of the correct type. Error: {e}"
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message)

    return translation_dictionary


def update(
    main_dictionary: Dict[str, TranslationDictionary],
    tmp_dictionary: Dict[str, TranslationDictionary],
) -> None:
    """
    Update a toolbox translation dictionary with another temporary translation dictionary

    Args:
        main_dictionary: the main toolbox translation dictionary containing past
            translation results
        tmp_dictionary: a temporary toolbox translation dictionary containing new translation
    Returns:

    """
    for standardized_phrase, translation in tmp_dictionary.items():
        try:
            main_dictionary_entry = main_dictionary[standardized_phrase]
            main_dictionary_entry.translated_phrase = translation.translated_phrase
            main_dictionary_entry.detected_language = translation.detected_language

        except KeyError:
            main_dictionary[standardized_phrase] = TranslationDictionary(
                standardized_phrase=standardized_phrase,
                translated_phrase=translation.translated_phrase,
                detected_language=translation.detected_language,
                original_phrases=translation.original_phrases,
            )


def convert_to_mappings(dictionary: Dict[str, TranslationDictionary]) -> Dict[str, str]:
    """
    Transform a translation dictionary into a mapping of original phrases to translated phrases
    Args:
        dictionary: a toolbox translation dictionary

    Returns:
        a dictionary with original phrase as key and translate phrase as value
    """
    mapping_from_dictionary = {
        original_phrase: t.translated_phrase
        for t in dictionary.values()
        for original_phrase in t.original_phrases
    }
    return mapping_from_dictionary


def from_dataset(dataset: Dataset) -> Dict[str, TranslationDictionary]:
    """
    Stream a dictionary from Tamr

    Args:
        dataset: Tamr Dataset object

    Returns:
        A toolbox translation dictionary

    Raises:
        ValueError: if the provided `dataset` is not a toolbox translation dictionary dataset
        NameError: if the provided `dataset` does not contain all the attributes of a
            toolbox translation dictionary
        RuntimeError: if there is any other problem while reading the `dataset` as a
            toolbox translation dictionary
    """
    if dataset.key_attribute_names[0] != "standardized_phrase":
        error_message = f"Provided Tamr Dataset is not a toolbox translation dictionary"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    dictionary = {}
    for record in dataset.records():
        try:
            entry = TranslationDictionary(**record)
            # values are returned as a list of a single string, we change this to string
            entry.translated_phrase = entry.translated_phrase[0]
            entry.detected_language = entry.detected_language[0]

            # original phrases are stored on Tamr as lists, we save it as a set
            entry.original_phrases = set(entry.original_phrases)

        except NameError as e:
            error_message = (
                f"Supplied Tamr dataset is not in a toolbox translation dictionary format: {e}"
            )
            LOGGER.error(error_message)
            raise NameError(error_message)
        except Exception as e:
            error_message = f"Error while reading the Tamr dataset translation dictionary: {e}"
            LOGGER.error(error_message)
            raise RuntimeError(error_message)

        formatted_dictionary = {entry.standardized_phrase: entry}
        dictionary.update(formatted_dictionary)
    return dictionary


def to_dataset(
    dictionary: Dict[str, TranslationDictionary],
    *,
    dataset: Optional[Dataset] = None,
    datasets_collection: Optional[DatasetCollection] = None,
    target_language: Optional[str] = None,
    source_language: Optional[str] = None,
    create_dataset: bool = False,
) -> str:
    """
    Ingest a toolbox dictionary in Tamr, creates the source dataset if it doesn't exists

    Args:
        dictionary: a toolbox translation dictionary
        dataset: a Tamr client dataset
        datasets_collection: a Tamr client datasets collection
        target_language: the target language of the given dictionary
        source_language: the source language of the given dictionary
        create_dataset: flag to create or upsert to an existing translation dictionary
            source dataset

    Returns:
        The name of the created or updated Tamr Dataset

    Raises:
        ValueError: if `create_dataset` is False and `dataset` is not provided or is not a
            toolbox translation dictionary dataset.
            If `create_dataset` is True but `datasets_collection` or `target_language` or
            `source_language` is missing or the Tamr dataset already exists
        RuntimeError: if there is an error during the creation of the Tamr dataset attributes
    """
    if create_dataset is False:
        if dataset is None:
            error_message = (
                "Tamr Client Dataset missing from inputs, please provide a Tamr "
                "Client Dataset if updating an existing translation dictionary dataset"
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        if dataset.key_attribute_names[0] != "standardized_phrase":
            error_message = f"Provided Tamr Dataset is not a toolbox translation dictionary"
            LOGGER.error(error_message)
            raise ValueError(error_message)

    else:
        if not (datasets_collection and target_language and source_language):
            error_message = (
                "A Tamr Datasets Collection, target_language and source_language "
                "must all be inputs if creating the toolbox translation "
                "dictionary dataset"
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        # Get dataset name using filename function
        # The value of dictionary folder here is unimportant
        dataset_name = os.path.basename(
            filename(
                dictionary_folder="not/a/real/path",  # will be dropped immediately
                target_language=target_language,
                source_language=source_language,
            )
        )
        if dataset_name in [d.name for d in datasets_collection]:
            error_message = (
                f"Tamr Dataset {dataset_name} already exists on Tamr, you cannot "
                f"create a dataset with the same name as another one"
            )
            LOGGER.error(error_message)
            raise ValueError(error_message)

        LOGGER.info(f"Creating toolbox translation dictionary dataset {dataset_name} on Tamr")
        creation_spec = {"name": dataset_name, "keyAttributeNames": ["standardized_phrase"]}
        dataset = datasets_collection.create(creation_spec)

        attributes = dataset.attributes
        for attribute in ["translated_phrase", "detected_language", "original_phrases"]:
            attr_spec = {
                "name": attribute,
                "type": {"baseType": "ARRAY", "innerType": {"baseType": "STRING"}},
            }
            try:
                attributes.create(attr_spec)
            except HTTPError as e:
                error_message = (
                    f"Error while creating attribute {attribute} for dataset {dataset_name}: {e}"
                )
                LOGGER.error(error_message)
                raise RuntimeError(error_message)

    LOGGER.info("Ingesting toolbox translation dictionary to Tamr")
    dataset.upsert_records(
        records=to_dict(dictionary), primary_key_name="standardized_phrase",
    )
    return dataset.name
