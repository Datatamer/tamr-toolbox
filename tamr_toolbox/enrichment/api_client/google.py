"""Tasks related to translating data with the google translation API"""
from typing import List, Dict, Optional
from tamr_toolbox.enrichment.dictionary import TranslationDictionary

import logging
import time
import html
import os

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke doc_src` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    from google.cloud.translate_v2 import Client as GoogleTranslateClient

LOGGER = logging.getLogger(__name__)


def _check_valid_translation_language(
    client: "GoogleTranslateClient", language: str, *, target_language: Optional[str] = None
) -> None:
    """
    Checks that the provided language is an accepted google translation api language with an
    option to specify the target language to check the source to target language combination
    is supported

    Args:
        client: a google translate api client
        language: the language to check
        target_language: the target language to translate to

    Returns:

    """
    languages = client.get_languages(target_language=target_language)
    valid_source_languages = [language["language"] for language in languages]
    if language not in valid_source_languages:
        if language != "auto":
            if target_language is None:
                error_message = (
                    f"Specified language {language} is not supported by the Google Translation API"
                    f"Valid languages are: {valid_source_languages}"
                )
            else:
                error_message = (
                    f"Translation from {language} to {target_language} is supported by the "
                    f"Google Translation API. "
                    f"Valid source languages for {target_language} are: {valid_source_languages}"
                )
            LOGGER.error(error_message)
            raise ValueError(error_message)


def _check_valid_translation_languages(
    client: "GoogleTranslateClient", source_language: str, target_language: str,
) -> None:
    """
    Checks that the provided target and source language combination is an accepted translation
    with the google translation api

    Args:
        client: a google translate api client
        source_language: the target language to translate from
        target_language: the target language to translate to

    Returns:

    """
    if target_language == "auto":
        error_message = "'auto' is not a valid target language for translation"
        LOGGER.error(error_message)
        raise ValueError(error_message)
    else:
        _check_valid_translation_language(client, target_language)

    if source_language == "auto":
        LOGGER.info(
            f"Source language is set to 'auto', "
            f"the Google Translation API will automatically detect the source language"
        )
    else:
        _check_valid_translation_language(client, source_language, target_language=target_language)


def translation_client_from_json(json_credential_path: str) -> "GoogleTranslateClient":
    """
    Returns a Google translation client based on credentials stored in a Google credential
    json file

    Args:
        json_credential_path: path to the google credential json file

    Returns:
        A Google Translate Client

    """
    from google.cloud.translate_v2 import Client as GoogleTranslateClient

    LOGGER.info("Connecting to Google Translation Client")
    google_client = GoogleTranslateClient.from_service_account_json(json_credential_path)
    return google_client


def translate(
    phrases_to_translate: List[str],
    client: "GoogleTranslateClient",
    *,
    source_language: str = "auto",
    target_language: str = "en",
    translation_model: str = "nmt",
    num_of_tries: int = 4,
) -> Optional[Dict[str, TranslationDictionary]]:
    """
    Translate a list of text to a target language using google's translation api

    Args:
        phrases_to_translate: list of phrases to translate from the source language to the
            target language
        client: location of the credentials JSON read by the google_api client
        source_language: the language the text to translate is in, "auto" means the api_client
            google_api api_client will try to detect the source language automatically
        target_language: the language to translate into
        translation_model: google_api api_client api_client model to use, "nmt" or "pbmt".
            Choose "pbmt" if an "nmt" model doesn't exists for your source to target language pair
        num_of_tries: number of times to try to translate if the translation call fails

    Returns:
        A toolbox translation dictionary.
        None if the translation failed
    """
    _check_valid_translation_languages(
        client=client, target_language=target_language, source_language=source_language
    )

    if source_language == "auto":
        source_language = None

    num_attempts = 1
    last_attempt = False
    while num_attempts <= num_of_tries:
        if num_attempts == num_of_tries and num_of_tries > 1:
            LOGGER.warning(
                f"WARNING: Tried and failed to translate current "
                f"chunk of phrases {num_of_tries - 1} times. Final try."
            )
            last_attempt = True
        try:
            response = client.translate(
                target_language=target_language,
                source_language=source_language,
                model=translation_model,
                values=phrases_to_translate,
            )
            if source_language is None:
                returned_translation = {
                    translation["input"]: TranslationDictionary(
                        standardized_phrase=translation["input"],
                        translated_phrase=html.unescape(translation["translatedText"]),
                        detected_language=translation["detectedSourceLanguage"],
                    )
                    for translation in response
                }
            else:
                returned_translation = {
                    translation["input"]: TranslationDictionary(
                        standardized_phrase=translation["input"],
                        translated_phrase=html.unescape(translation["translatedText"]),
                    )
                    for translation in response
                }
            LOGGER.debug(returned_translation)
            return returned_translation

        # TODO: check which exception returns the User Rate Limit error
        #  to better handle the exceptions
        except Exception as e:
            if "User Rate Limit Exceeded" in str(e) and not last_attempt:
                LOGGER.warning(
                    "Google api_client API user rate limit exceeded, "
                    "waiting 10 seconds and retrying."
                )
                time.sleep(10)
                num_attempts += 1
                continue
            else:
                error_message = f"Could not translate current chunk of phrases. Error: {e}"
                LOGGER.error(error_message)
                num_attempts += 1
                continue
    else:
        error_message = f"Ran out of number of tries. Skipping."
        LOGGER.error(error_message)
        return None
