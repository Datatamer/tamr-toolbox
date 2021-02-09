"""Tasks related to efficiently translating data not present in existing translation
dictionaries"""
from typing import Union, List, Any, Dict
from tamr_toolbox.enrichment.dictionary import TranslationDictionary

from tamr_toolbox.enrichment.dictionary import update, save
from tamr_toolbox.enrichment.api_client import google

import math
import logging
import os

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    from google.cloud.translate_v2 import Client as GoogleTranslateClient


LOGGER = logging.getLogger(__name__)


def _yield_chunk(list_to_split: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into a List of List with constant length

    Args:
        list_to_split: List to split into chunks
        chunk_size: number of items to have in each list after splitting

    Returns:
        A List of List
    """

    # For item i in a range that is a length of l,
    for i in range(0, len(list_to_split), chunk_size):
        # Create an index range for l of n items:
        yield list_to_split[i : i + chunk_size]


def _filter_numeric_and_null_phrases(phrase: Union[str, None]) -> str:
    """
    Transform None and numbers saved as text as empty strings

    Args:
        phrase: data to filter

    Returns:
        An empty string
        Raises an error if the input is neither None nor a string

    Raises:
        TypeError: is the provided phrase is not of type string
    """
    if phrase is None:
        return ""
    elif not isinstance(phrase, str):
        error_message = (
            f"{phrase} is not in text format. " f"Only text can be translated, check data type."
        )
        LOGGER.error(error_message)
        raise TypeError(error_message)
    elif phrase.isnumeric():
        return ""
    else:
        return phrase


def standardize_phrases(original_phrases: List[str]) -> List[str]:
    """
    Standardize phrases to translate to avoid re-translating previously translated phrases but
    with different formating

    Args:
        original_phrases: List of phrases to standardize

    Returns:
        List of standardized text
    """
    standardized_phrases = [
        " ".join(_filter_numeric_and_null_phrases(phrase).lower().split())
        for phrase in original_phrases
    ]
    return standardized_phrases


def get_phrases_to_translate(
    original_phrases: List[str], translation_dictionary: Dict[str, TranslationDictionary]
) -> List[str]:
    """
    Find phrases not previously translated and initiate dictionary entry

    Args:
        original_phrases: list of phrases to translate
        translation_dictionary: a translation dictionary

    Returns:
        List of standardized phrases not present as keys of the translation dictionary

    """
    count_already_translated = 0
    count_needing_translation = 0

    for original, standard in zip(original_phrases, standardize_phrases(original_phrases)):
        if standard in translation_dictionary.keys():
            translation_dictionary[standard].original_phrases.add(original)
            count_already_translated += 1
        else:
            translation_dictionary[standard] = TranslationDictionary(
                original_phrases={original}, standardized_phrase=standard
            )
            count_needing_translation += 1

    LOGGER.info(
        f"From the {len(original_phrases)} sent for translation, "
        f"{count_already_translated} can be translated with the dictionary and "
        f"{count_needing_translation} need to be translated"
    )

    to_translate = [
        t.standardized_phrase
        for t in translation_dictionary.values()
        if t.translated_phrase is None
    ]
    LOGGER.debug(f"{to_translate}")
    return to_translate


def from_list(
    all_phrases: List[str],
    client: "GoogleTranslateClient",
    dictionary: Dict[str, TranslationDictionary],
    *,
    source_language: str = "auto",
    target_language: str = "en",
    chunk_size: int = 100,
    translation_model: str = "nmt",
    intermediate_save_every_n_chunks: Union[int, None] = None,
    intermediate_save_to_disk: bool = False,
    intermediate_folder: str = "/tmp",
) -> Dict[str, TranslationDictionary]:
    """
    Translate a list of phrases from source language to target language.
    The translation is saved in a dictionary on your local file system before updating the
    main dictionary

    Args:
        all_phrases: List of standardized phrases to translate.
        client: a google translate api client
        dictionary: a toolbox translation dictionary
        source_language: the language the text to translate is in, "auto" means the api_client
            google_api api_client will try to detect the source language automatically
        target_language: the language to translate into
        chunk_size: number of phrases to translate per api_client calls, set too high and you
            will hit API user rate limit errors
        translation_model: google_api api_client api_client model to use, "nmt" or "pbmt".
            Choose "pbmt" if an "nmt" model doesn't exists for your source to target language pair
        intermediate_save_every_n_chunks: save periodically api_client dictionary to disk every n
            chunk of phrases translated
        intermediate_save_to_disk: decide whether to save periodically the dictionary to disk to
            avoid loss of translation data if code breaks
        intermediate_folder: path to folder where dictionary will be save periodically to avoid
            loss of translation data

    Returns:
        The updated translation dictionary

    Raises:
        ValueError: if the argument chunk_size is set to 0
    """
    if chunk_size == 0:
        error_message = "Translation chunk size cannot be of size 0"
        LOGGER.error(error_message)
        raise ValueError(error_message)

    if intermediate_save_every_n_chunks == 0 or intermediate_save_every_n_chunks is None:
        intermediate_save_every_n_chunks = math.inf

    unique_all_phrases = list(set(all_phrases))
    nbr_of_unique_phrases = len(unique_all_phrases)

    phrases_to_translate = get_phrases_to_translate(unique_all_phrases, dictionary)
    number_phrases_to_translate = len(phrases_to_translate)

    if number_phrases_to_translate == 0:
        LOGGER.info("All phrases to translate are found in the local dictionary.")

    else:
        LOGGER.info(
            f"Of the {nbr_of_unique_phrases} unique phrases to translate, "
            f"{number_phrases_to_translate} were not found in the dictionary."
        )

        # Google has a translation rate limits
        # to avoid hitting those the phrases are sent for translation in chunks
        number_of_chunks = math.ceil(number_phrases_to_translate / chunk_size)

        tmp_dictionary = {}
        for ix, chunk_of_phrases in enumerate(_yield_chunk(phrases_to_translate, chunk_size)):
            LOGGER.debug(f"Translating chunk {ix + 1} out of {number_of_chunks}.")
            translated_phrases = google.translate(
                phrases_to_translate=chunk_of_phrases,
                client=client,
                source_language=source_language,
                target_language=target_language,
                translation_model=translation_model,
            )
            if translated_phrases is not None:
                tmp_dictionary.update(translated_phrases)

            if (ix % intermediate_save_every_n_chunks) == 0:
                LOGGER.info("Saving intermediate outputs")
                update(
                    main_dictionary=dictionary, tmp_dictionary=tmp_dictionary,
                )
                if intermediate_save_to_disk:
                    save(
                        translation_dictionary=dictionary,
                        dictionary_folder=intermediate_folder,
                        target_language=target_language,
                        source_language=source_language,
                    )
                # resetting temporary results after saving it
                tmp_dictionary = {}

        # update dictionary
        update(
            main_dictionary=dictionary, tmp_dictionary=tmp_dictionary,
        )

    return dictionary
