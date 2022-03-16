"""Tests for tasks related to creating, updating, saving and moving translation dictionaries
in and out of Tamr"""
from tamr_toolbox.enrichment.dictionary import TranslationDictionary

from tamr_toolbox import utils
from tamr_toolbox import enrichment

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

from pathlib import Path
from typing import Optional
import tempfile
import pytest


CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
DICTIONARY_DATASET_ID = CONFIG["datasets"]["dictionary_auto_to_fr.json"]


# Raw export of minimal_schema_mapping_unified_dataset
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


TEST_TRANSLATION_DICTIONARY_JSON = [
    "{"
    '"standardized_phrase": "cheddar cheese", '
    '"translated_phrase": "fromage cheddar", '
    '"detected_language": "en", "original_phrases": ["cheddar cheese"]'
    "}",
    "{"
    '"standardized_phrase": "ground beef", '
    '"translated_phrase": "boeuf hach\\u00e9", '
    '"detected_language": "en", '
    '"original_phrases": ["ground beef"]'
    "}",
]


TEST_TRANSLATION_DICTIONARY_DICT = [
    {
        "standardized_phrase": "cheddar cheese",
        "translated_phrase": "fromage cheddar",
        "detected_language": "en",
        "original_phrases": ["cheddar cheese"],
    },
    {
        "standardized_phrase": "ground beef",
        "translated_phrase": "boeuf haché",
        "detected_language": "en",
        "original_phrases": ["ground beef"],
    },
]


def test_dictionary_filename():
    dictionary_folder = Path("/test/dictionary")
    target_language = "fr"
    source_language = "auto"

    dictionary_path = Path("/test/dictionary") / "dictionary_auto_to_fr.json"
    assert str(dictionary_path) == enrichment.dictionary.filename(
        dictionary_folder, target_language=target_language, source_language=source_language
    )


def test_dictionary_creating_and_loading():
    with tempfile.TemporaryDirectory() as tempdir:
        dictionary_folder = Path(tempdir)
        target_language = "fr"
        source_language = "auto"

        empty_dictionary_filepath = enrichment.dictionary.create(
            dictionary_folder, target_language=target_language, source_language=source_language
        )
        assert empty_dictionary_filepath == str(dictionary_folder / "dictionary_auto_to_fr.json")

        empty_dictionary = enrichment.dictionary.load(
            dictionary_folder, target_language=target_language, source_language=source_language,
        )
        assert empty_dictionary == {}


def test_dictionary_saving_and_loading():
    with tempfile.TemporaryDirectory() as tempdir:
        dictionary_folder = Path(tempdir)
        target_language = "fr"
        source_language = "auto"

        enrichment.dictionary.save(
            translation_dictionary=TEST_TRANSLATION_DICTIONARY,
            dictionary_folder=dictionary_folder,
            target_language=target_language,
            source_language=source_language,
        )

        saved_dictionary = enrichment.dictionary.load(
            dictionary_folder, target_language=target_language, source_language=source_language,
        )
        assert TEST_TRANSLATION_DICTIONARY == saved_dictionary


def test_dictionary_updating():
    main_dictionary = {}
    for key, value in TEST_TRANSLATION_DICTIONARY.items():
        main_dictionary.update(
            {
                key: TranslationDictionary(
                    standardized_phrase=key, original_phrases=value.original_phrases
                )
            }
        )
    enrichment.dictionary.update(
        main_dictionary, TEST_TRANSLATION_DICTIONARY,
    )
    assert main_dictionary == TEST_TRANSLATION_DICTIONARY

    # Test updating dictionary with missing keys
    main_dictionary = {}
    enrichment.dictionary.update(
        main_dictionary, TEST_TRANSLATION_DICTIONARY,
    )
    assert main_dictionary == TEST_TRANSLATION_DICTIONARY


def test_dictionary_convert_to_mappings():
    expected_results = {
        "cheddar cheese": "fromage cheddar",
        "Cheddar Cheese": "fromage cheddar",
        "ground beef": "boeuf haché",
        "Ground beef": "boeuf haché",
        "whole chicken": None,
    }
    assert expected_results == enrichment.dictionary.convert_to_mappings(
        TEST_TRANSLATION_DICTIONARY_EXTENDED
    )


def test_dictionary_to_json():
    assert (
        enrichment.dictionary.to_json(TEST_TRANSLATION_DICTIONARY)
        == TEST_TRANSLATION_DICTIONARY_JSON
    )


def test_dictionary_to_dict():
    assert (
        enrichment.dictionary.to_dict(TEST_TRANSLATION_DICTIONARY)
        == TEST_TRANSLATION_DICTIONARY_DICT
    )


@mock_api()
def test_dictionary_from_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(DICTIONARY_DATASET_ID)
    assert enrichment.dictionary.from_dataset(dataset) == TEST_TRANSLATION_DICTIONARY


@mock_api()
def test_dictionary_from_dataset_with_wrong_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(
        CONFIG["datasets"]["minimal_golden_records_golden_records"]
    )
    with pytest.raises(ValueError):
        enrichment.dictionary.from_dataset(dataset)


@mock_api()
def test_dictionary_to_and_from_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset_name = enrichment.dictionary.to_dataset(
        dictionary=TEST_TRANSLATION_DICTIONARY,
        create_dataset=True,
        datasets_collection=client.datasets,
        source_language="en",
        target_language="fr",
    )
    dataset = client.datasets.by_name(dataset_name)
    assert enrichment.dictionary.from_dataset(dataset) == TEST_TRANSLATION_DICTIONARY
    dataset.delete()


@mock_api()
def test_dictionary_to_dataset_upsert():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(DICTIONARY_DATASET_ID)
    dataset_name = enrichment.dictionary.to_dataset(
        dictionary=TEST_TRANSLATION_DICTIONARY, dataset=dataset,
    )
    assert dataset_name == "dictionary_auto_to_fr.json"
    assert enrichment.dictionary.from_dataset(dataset) == TEST_TRANSLATION_DICTIONARY


@mock_api()
def test_dictionary_to_dataset_upsert_wrong_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(
        CONFIG["datasets"]["minimal_golden_records_golden_records"]
    )
    with pytest.raises(ValueError):
        enrichment.dictionary.to_dataset(
            dictionary=TEST_TRANSLATION_DICTIONARY, dataset=dataset,
        )


@mock_api()
def test_dictionary_to_dataset_create_existing_dataset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    with pytest.raises(ValueError):
        enrichment.dictionary.to_dataset(
            dictionary=TEST_TRANSLATION_DICTIONARY,
            create_dataset=True,
            datasets_collection=client.datasets,
            source_language="auto",
            target_language="fr",
        )


@pytest.mark.parametrize(
    "tamr_dataset, tamr_datasets, source_language, target_language, create_dataset",
    [
        (None, None, None, None, False),
        (None, "test", None, None, True),
        (None, None, "test", None, True),
        (None, None, None, "test", True),
        (None, None, "test", "test", True),
    ],
)
def test_dictionary_to_dataset_missing_inputs(
    tamr_dataset: Optional[str],
    tamr_datasets: Optional[str],
    target_language: Optional[str],
    source_language: Optional[str],
    create_dataset: bool,
):
    with pytest.raises(ValueError):
        enrichment.dictionary.to_dataset(
            dictionary=TEST_TRANSLATION_DICTIONARY,
            dataset=tamr_dataset,
            datasets_collection=tamr_datasets,
            source_language=source_language,
            target_language=target_language,
            create_dataset=create_dataset,
        )
