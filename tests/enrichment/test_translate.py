"""Tests for tasks related to efficiently translating data not present in existing translation
dictionaries"""
from typing import List, Dict, Optional
from tamr_toolbox.enrichment.dictionary import TranslationDictionary

from tamr_toolbox import utils
from tamr_toolbox import enrichment

from unittest.mock import MagicMock, patch
from tests._common import get_toolbox_root_dir

import pytest
import tempfile
from google.cloud.translate_v2 import Client as GoogleTranslateClient


CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

TEST_TRANSLATION_DICTIONARY = {
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese",
        translated_phrase="fromage cheddar",
        detected_language="en",
        original_phrases={"cheddar cheese"},
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef",
        translated_phrase="boeuf haché",
        detected_language="en",
        original_phrases={"ground beef"},
    ),
}

TEST_TRANSLATION_DICTIONARY_EXTENDED = {
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese",
        translated_phrase="fromage cheddar",
        detected_language="en",
        original_phrases={"cheddar cheese", "Cheddar Cheese"},
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef",
        translated_phrase="boeuf haché",
        detected_language="en",
        original_phrases={"ground beef", "Ground beef"},
    ),
    "whole chicken": TranslationDictionary(
        standardized_phrase="whole chicken",
        translated_phrase=None,
        detected_language=None,
        original_phrases={"whole chicken"},
    ),
}

TEST_TRANSLATION_DICTIONARY_TRANSLATED = {
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese",
        translated_phrase="fromage cheddar",
        detected_language="en",
        original_phrases={"cheddar cheese", "Cheddar Cheese"},
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef",
        translated_phrase="boeuf haché",
        detected_language="en",
        original_phrases={"ground beef", "Ground beef"},
    ),
    "whole chicken": TranslationDictionary(
        standardized_phrase="whole chicken",
        translated_phrase="poulet entier",
        detected_language="en",
        original_phrases={"whole chicken"},
    ),
}


def _mock_translate_response(
    target_language: str, source_language: str, model: str, values: List[str],
) -> List[Dict[str, str]]:
    """
    A simulated response for the Google Translate Client get_languages() call
    https://github.com/googleapis/python-translate/blob/master/google/cloud/translate_v2/client.py

    Args:
        target_language: The language to translate to (ignored in mock response)
        source_language: The language to translate from
        model: The model used by the google client for language detection
        values: The values to detect the language for
    Returns:
        A list of JSON responses per input value
    """

    if sum(len(value) for value in values) > 100000:
        raise RuntimeError("User Rate Limit Exceeded")

    translated_values = {
        "cheddar cheese": "fromage cheddar",
        "ground beef": "boeuf haché",
        "skim milk": "lait écrémé",
        "whole chicken": "poulet entier",
        "bacon": "Bacon",
        "american cheese": "Fromage Américain",
        "roast beef": "rôti de bœuf",
        "boneless chicken breasts": "poitrines de poulet désossées",
        "swiss cheese": "fromage suisse",
    }
    mock_response = []
    for value in values:
        if source_language is None:
            mock_response.append(
                {
                    "translatedText": translated_values[value],
                    "detectedSourceLanguage": "en",
                    "model": model,
                    "input": value,
                }
            )
        else:
            mock_response.append(
                {"translatedText": translated_values[value], "model": model, "input": value}
            )
    return mock_response


def _mock_get_languages_response(target_language: Optional[str] = None) -> List[Dict[str, str]]:
    """
    A simulated response for the Google Translate Client translate() call
    https://github.com/googleapis/python-translate/blob/master/google/cloud/translate_v2/client.py

    Args:
        target_language: The language to translate to (ignored in mock response)
    Returns:
        A list of JSON responses per source language
    """
    mock_response = [
        {"language": "af", "name": "Afrikaans"},
        {"language": "sq", "name": "Albanian"},
        {"language": "am", "name": "Amharic"},
        {"language": "ar", "name": "Arabic"},
        {"language": "hy", "name": "Armenian"},
        {"language": "az", "name": "Azerbaijani"},
        {"language": "eu", "name": "Basque"},
        {"language": "be", "name": "Belarusian"},
        {"language": "bn", "name": "Bengali"},
        {"language": "bs", "name": "Bosnian"},
        {"language": "bg", "name": "Bulgarian"},
        {"language": "ca", "name": "Catalan"},
        {"language": "ceb", "name": "Cebuano"},
        {"language": "ny", "name": "Chichewa"},
        {"language": "zh-CN", "name": "Chinese (Simplified)"},
        {"language": "zh-TW", "name": "Chinese (Traditional)"},
        {"language": "co", "name": "Corsican"},
        {"language": "hr", "name": "Croatian"},
        {"language": "cs", "name": "Czech"},
        {"language": "da", "name": "Danish"},
        {"language": "nl", "name": "Dutch"},
        {"language": "en", "name": "English"},
        {"language": "eo", "name": "Esperanto"},
        {"language": "et", "name": "Estonian"},
        {"language": "tl", "name": "Filipino"},
        {"language": "fi", "name": "Finnish"},
        {"language": "fr", "name": "French"},
        {"language": "fy", "name": "Frisian"},
        {"language": "gl", "name": "Galician"},
        {"language": "ka", "name": "Georgian"},
        {"language": "de", "name": "German"},
        {"language": "el", "name": "Greek"},
        {"language": "gu", "name": "Gujarati"},
        {"language": "ht", "name": "Haitian Creole"},
        {"language": "ha", "name": "Hausa"},
        {"language": "haw", "name": "Hawaiian"},
        {"language": "iw", "name": "Hebrew"},
        {"language": "hi", "name": "Hindi"},
        {"language": "hmn", "name": "Hmong"},
        {"language": "hu", "name": "Hungarian"},
        {"language": "is", "name": "Icelandic"},
        {"language": "ig", "name": "Igbo"},
        {"language": "id", "name": "Indonesian"},
        {"language": "ga", "name": "Irish"},
        {"language": "it", "name": "Italian"},
        {"language": "ja", "name": "Japanese"},
        {"language": "jw", "name": "Javanese"},
        {"language": "kn", "name": "Kannada"},
        {"language": "kk", "name": "Kazakh"},
        {"language": "km", "name": "Khmer"},
        {"language": "rw", "name": "Kinyarwanda"},
        {"language": "ko", "name": "Korean"},
        {"language": "ku", "name": "Kurdish (Kurmanji)"},
        {"language": "ky", "name": "Kyrgyz"},
        {"language": "lo", "name": "Lao"},
        {"language": "la", "name": "Latin"},
        {"language": "lv", "name": "Latvian"},
        {"language": "lt", "name": "Lithuanian"},
        {"language": "lb", "name": "Luxembourgish"},
        {"language": "mk", "name": "Macedonian"},
        {"language": "mg", "name": "Malagasy"},
        {"language": "ms", "name": "Malay"},
        {"language": "ml", "name": "Malayalam"},
        {"language": "mt", "name": "Maltese"},
        {"language": "mi", "name": "Maori"},
        {"language": "mr", "name": "Marathi"},
        {"language": "mn", "name": "Mongolian"},
        {"language": "my", "name": "Myanmar (Burmese)"},
        {"language": "ne", "name": "Nepali"},
        {"language": "no", "name": "Norwegian"},
        {"language": "or", "name": "Odia (Oriya)"},
        {"language": "ps", "name": "Pashto"},
        {"language": "fa", "name": "Persian"},
        {"language": "pl", "name": "Polish"},
        {"language": "pt", "name": "Portuguese"},
        {"language": "pa", "name": "Punjabi"},
        {"language": "ro", "name": "Romanian"},
        {"language": "ru", "name": "Russian"},
        {"language": "sm", "name": "Samoan"},
        {"language": "gd", "name": "Scots Gaelic"},
        {"language": "sr", "name": "Serbian"},
        {"language": "st", "name": "Sesotho"},
        {"language": "sn", "name": "Shona"},
        {"language": "sd", "name": "Sindhi"},
        {"language": "si", "name": "Sinhala"},
        {"language": "sk", "name": "Slovak"},
        {"language": "sl", "name": "Slovenian"},
        {"language": "so", "name": "Somali"},
        {"language": "es", "name": "Spanish"},
        {"language": "su", "name": "Sundanese"},
        {"language": "sw", "name": "Swahili"},
        {"language": "sv", "name": "Swedish"},
        {"language": "tg", "name": "Tajik"},
        {"language": "ta", "name": "Tamil"},
        {"language": "tt", "name": "Tatar"},
        {"language": "te", "name": "Telugu"},
        {"language": "th", "name": "Thai"},
        {"language": "tr", "name": "Turkish"},
        {"language": "tk", "name": "Turkmen"},
        {"language": "uk", "name": "Ukrainian"},
        {"language": "ur", "name": "Urdu"},
        {"language": "ug", "name": "Uyghur"},
        {"language": "uz", "name": "Uzbek"},
        {"language": "vi", "name": "Vietnamese"},
        {"language": "cy", "name": "Welsh"},
        {"language": "xh", "name": "Xhosa"},
        {"language": "yi", "name": "Yiddish"},
        {"language": "yo", "name": "Yoruba"},
        {"language": "zu", "name": "Zulu"},
        {"language": "he", "name": "Hebrew"},
        {"language": "zh", "name": "Chinese (Simplified)"},
    ]
    return mock_response


@pytest.mark.parametrize(
    "chunk_size, expected_result",
    [(1, [["a"], ["b"], ["c"]]), (2, [["a", "b"], ["c"]]), (100, [["a", "b", "c"]])],
)
def test_translate__yield_chunk(chunk_size: int, expected_result: List[List[str]]):
    a_list_of_tests = ["a", "b", "c"]
    assert expected_result == list(enrichment.translate._yield_chunk(a_list_of_tests, chunk_size))


def test_translate__filter_numeric_and_null_phrases():
    a_simple_list = [None, "2", "a test phrase"]
    expected_result = ["", "", "a test phrase"]
    for test, result in zip(a_simple_list, expected_result):
        assert result == enrichment.translate._filter_numeric_and_null_phrases(test)
    with pytest.raises(TypeError):
        enrichment.translate._filter_numeric_and_null_phrases(2)


def test_translate_standardize_phrases():
    a_list_of_tests = ["My first test", "my first test", "       my       first Test"]
    expected_result = ["my first test", "my first test", "my first test"]
    assert expected_result == enrichment.translate.standardize_phrases(a_list_of_tests)


def test_translate_get_phrases_to_translate():
    phrases_to_translate = ["Cheddar Cheese", "Ground beef", "whole chicken"]
    expected_to_translate = ["whole chicken"]

    to_translate = enrichment.translate.get_phrases_to_translate(
        original_phrases=phrases_to_translate, translation_dictionary=TEST_TRANSLATION_DICTIONARY,
    )

    assert expected_to_translate == to_translate
    assert TEST_TRANSLATION_DICTIONARY == TEST_TRANSLATION_DICTIONARY_EXTENDED


@patch("google.cloud.translate_v2.Client")
@pytest.mark.parametrize(
    "chunk_size, intermediate_save_every_n_chunks, intermediate_save_to_disk",
    [
        (100, None, False),
        (1, None, False),
        (100, 1, True),
        (100, 100, True),
        (1, 100, True),
        (1, 1, True),
    ],
)
def test_translate_from_list(
    Client: GoogleTranslateClient,
    chunk_size: int,
    intermediate_save_every_n_chunks: Optional[int],
    intermediate_save_to_disk: bool,
):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    Client().translate = MagicMock(side_effect=_mock_translate_response)
    mock_client = Client()

    phrases_to_translate = ["Cheddar Cheese", "Ground beef", "whole chicken"]
    with tempfile.TemporaryDirectory() as tempdir:
        dictionary = enrichment.translate.from_list(
            all_phrases=phrases_to_translate,
            client=mock_client,
            dictionary=TEST_TRANSLATION_DICTIONARY,
            source_language="auto",
            target_language="fr",
            chunk_size=chunk_size,
            intermediate_save_every_n_chunks=intermediate_save_every_n_chunks,
            intermediate_save_to_disk=intermediate_save_to_disk,
            intermediate_folder=tempdir,
        )
    assert dictionary == TEST_TRANSLATION_DICTIONARY_TRANSLATED
