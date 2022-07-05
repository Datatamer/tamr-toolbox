"""An example script to translate data from disk and save results on disk"""
from typing import List

import tamr_toolbox as tbox
import pandas as pd

import argparse


def main(
    json_credential_path: str,
    dictionary_folder: str,
    attributes_to_translate: List[str],
    path_to_csv_to_translate: str,
    path_to_translated_csv: str,
) -> None:
    """
    Translate data located on disk and save results to disk
    Args:
        json_credential_path: path to the json file containing Google Translate API keys
        dictionary_folder: Path to the folder on disk where local versions of dictionary are saved
        attributes_to_translate: List of attributes from the local csv file to translate
        path_to_csv_to_translate: Path to the CSV file to translate
        path_to_translated_csv: path to the CSV file with translated data

    Returns:

    """
    # make Google api translation client
    google = tbox.enrichment.api_client.google.translation_client_from_json(json_credential_path)

    # read csv file from disk
    df = pd.read_csv(path_to_csv_to_translate, dtype=object)

    # load dictionary
    LOGGER.info(f"Starting translation from french to english")
    dictionary = tbox.enrichment.dictionary.load(
        dictionary_folder=dictionary_folder, target_language="en", source_language="fr"
    )

    # translate attribute by attribute
    for attribute in attributes_to_translate:
        LOGGER.info(f"Translating attribute: {attribute}")
        dictionary = tbox.enrichment.translate.from_list(
            all_phrases=df[attribute].unique().tolist(),
            client=google,
            dictionary=dictionary,
            target_language="en",
            source_language="fr",
            intermediate_save_every_n_chunks=100,
            intermediate_save_to_disk=True,
            intermediate_folder=dictionary_folder,
        )

    # save to disk new dictionary with added translation
    LOGGER.info(f"Finished translation from french to english")
    LOGGER.info(f"Saving updated dictionary to disk")
    tbox.enrichment.dictionary.save(
        translation_dictionary=dictionary,
        dictionary_folder=dictionary_folder,
        target_language="en",
        source_language="fr",
    )

    # Translating dataframe insitu
    LOGGER.info(f"Translating dataframe from french to english")
    LOGGER.debug("Converting dictionary to mapping of original to translated phrases")
    dictionary = tbox.enrichment.dictionary.convert_to_mappings(dictionary)

    for attribute in attributes_to_translate:
        LOGGER.info(f"Translating attribute {attribute} from french to english")
        df[attribute + "_translated"] = df[attribute].map(dictionary)

    # Then save dataframe to disk
    df.to_csv(path_to_translated_csv, index=False)


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/conf/dataset.config.yaml"
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        json_credential_path=CONFIG["translation"]["json_credential_path"],
        dictionary_folder=CONFIG["path_to_dictionary_folder"],
        attributes_to_translate=CONFIG["translation"]["attributes"],
        path_to_csv_to_translate="path_to_my_data.csv",
        path_to_translated_csv="path_to_my_translated_data.csv",
    )
