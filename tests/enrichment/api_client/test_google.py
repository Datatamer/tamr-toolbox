"""Tests for asks related to translating data with the google translation API"""
from typing import Dict, List, Optional
from tamr_toolbox.enrichment.dictionary import TranslationDictionary

from tamr_toolbox import enrichment
from google.cloud.translate_v2 import Client as GoogleTranslateClient

from unittest.mock import MagicMock, patch

import pytest


# Raw export of minimal_schema_mapping_unified_dataset
TEST_TRANSLATION_DICTIONARY = {
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese", translated_phrase="fromage cheddar"
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef", translated_phrase="boeuf haché"
    ),
}

TEST_AUTO_TRANSLATION_DICTIONARY = {
    "cheddar cheese": TranslationDictionary(
        standardized_phrase="cheddar cheese",
        translated_phrase="fromage cheddar",
        detected_language="en",
    ),
    "ground beef": TranslationDictionary(
        standardized_phrase="ground beef", translated_phrase="boeuf haché", detected_language="en"
    ),
}


def _mock_translate_response(
    target_language: str, source_language: str, model: str, values: List[str]
) -> List[Dict[str, str]]:

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


@patch("google.cloud.translate_v2.Client")
def test_google_check_valid_translation_language(Client):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    mock_client = Client()
    enrichment.api_client.google._check_valid_translation_language(
        mock_client, "fr", target_language="en"
    )
    enrichment.api_client.google._check_valid_translation_language(
        mock_client, "auto", target_language="en"
    )
    enrichment.api_client.google._check_valid_translation_language(mock_client, "fr")


@patch("google.cloud.translate_v2.Client")
def test_google_check_valid_translation_language_incorrect_language(Client):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    mock_client = Client()

    with pytest.raises(ValueError):
        enrichment.api_client.google._check_valid_translation_language(
            mock_client, "a_language_that_does_not_exists"
        )

    with pytest.raises(ValueError):
        enrichment.api_client.google._check_valid_translation_language(
            mock_client, "a_language_that_does_not_exists", target_language="fr"
        )

    with pytest.raises(ValueError):
        enrichment.api_client.google._check_valid_translation_language(
            mock_client, "a_language_that_does_not_exists", target_language="auto"
        )


@patch("google.cloud.translate_v2.Client")
def test_translate(Client):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    Client().translate = MagicMock(side_effect=_mock_translate_response)
    mock_client = Client()

    tmp_dictionary = enrichment.api_client.google.translate(
        phrases_to_translate=["cheddar cheese", "ground beef"],
        client=mock_client,
        source_language="en",
        target_language="fr",
    )
    assert tmp_dictionary == TEST_TRANSLATION_DICTIONARY


@patch("google.cloud.translate_v2.Client")
def test_translate_with_auto_detect_source_language(Client):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    Client().translate = MagicMock(side_effect=_mock_translate_response)
    mock_client = Client()

    tmp_dictionary = enrichment.api_client.google.translate(
        phrases_to_translate=["cheddar cheese", "ground beef"],
        client=mock_client,
        source_language="auto",
        target_language="fr",
    )
    assert tmp_dictionary == TEST_AUTO_TRANSLATION_DICTIONARY


@patch("google.cloud.translate_v2.Client")
def test_translate_with_rate_limit_exceeded(Client):
    Client().get_languages = MagicMock(side_effect=_mock_get_languages_response)
    Client().translate = MagicMock(side_effect=_mock_translate_response)
    mock_client = Client()

    tmp_dictionary = enrichment.api_client.google.translate(
        phrases_to_translate=["".join(["a" for i in range(200000)])],
        client=mock_client,
        source_language="auto",
        target_language="fr",
    )
    assert tmp_dictionary is None


def test_translation_client_from_json():
    mock_client = GoogleTranslateClient
    mock_client.from_service_account_json = MagicMock(return_value=True)
    assert enrichment.api_client.google.translation_client_from_json("a_path_to_a_json_file")
