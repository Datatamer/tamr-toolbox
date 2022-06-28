"""An example script to translate data from Tamr and save results in Tamr"""
from typing import Dict, Any

import tamr_toolbox as tbox

import argparse


def main(
    *,
    instance_connection_info: Dict[str, Any],
    unified_dataset_id: str,
    dictionary_dataset_id: str,
) -> None:
    """
    Translate data streamed from Tamr and save results on Tamr

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        unified_dataset_id: id of the Tamr unified dataset containing the data to translate
        dictionary_dataset_id: id of the Tamr toolbox translation dictionary dataset

    Returns:

    """
    # make Tamr Client, make Google api translation client
    tamr = tbox.utils.client.create(**instance_connection_info)
    google = tbox.enrichment.api_client.google.translation_client_from_json(
        json_credential_path=CONFIG["translation"]["json_credential_path"]
    )

    # list attributes to translate
    attributes_to_translate = CONFIG["translation"]["attributes"]

    # get dataframe from Tamr unified dataset: best is to pass a delta dataset where
    # only untranslated data is kept.
    # To do this setup a SM project connected to your current translated UD and filter to records
    # with null values in the translated attributes.
    dataset = tamr.datasets.by_resource_id(unified_dataset_id)
    df = tbox.data_io.dataframe.from_dataset(
        dataset, columns=attributes_to_translate, flatten_delimiter=" | "
    )

    # stream dictionary from Tamr. Dictionaries should follow the TranslationDictionary class of
    # the toolbox: "standardized_phrase" (str), "translated_phrase" (str),
    # "detected_language" (str), "original_phrases" (List[str])
    dictionary_dataset = tamr.datasets.by_resource_id(dictionary_dataset_id)
    dictionary = tbox.enrichment.dictionary.from_dataset(dictionary_dataset)

    for column in df.columns:
        LOGGER.info(f"Translating attribute: {column}")
        dictionary = tbox.enrichment.translate.from_list(
            all_phrases=df[column].unique().tolist(),
            client=google,
            dictionary=dictionary,
            source_language="fr",
            target_language="en",
        )

    # update dictionary on Tamr
    dataset_name = tbox.enrichment.dictionary.to_dataset(
        dictionary=dictionary, dataset=dictionary_dataset
    )
    LOGGER.info(f"Tamr dataset {dataset_name} updated with new translation data")
    LOGGER.info("Script complete.")


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
        instance_connection_info=CONFIG["my_tamr_instance"],
        unified_dataset_id=CONFIG["datasets"]["my_mastering_project_dataset"]["id"],
        dictionary_dataset_id=CONFIG["datasets"]["my_dictionary"]["id"],
    )
