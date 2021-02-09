"""An example script to create an empty translation dictionary on Tamr"""
from typing import Dict, Any
import tamr_toolbox as tbox
import argparse


def main(
    *,
    instance_connection_info: Dict[str, Any],
    dictionary_folder: str,
    source_language: str,
    target_language: str,
) -> None:
    """
    Create an empty toolbox translation dictionary dataset on Tamr
    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dictionary_folder: Path to the folder on disk where local versions of dictionary are saved
        source_language: Source language of the dictionary
        target_language: Target language of the dictionary

    Returns:

    """
    # Connect to tamr
    tamr = tbox.utils.client.create(**instance_connection_info)

    # Create an empty dictionary on Tamr or load existing dictionary
    LOGGER.info(
        f"Initiating empty translation dictionary from source language {source_language} "
        f"to target language {target_language}"
    )

    dictionary = tbox.enrichment.dictionary.load(
        dictionary_folder=dictionary_folder,
        target_language=target_language,
        source_language=source_language,
    )

    if len(dictionary) > 0:
        error_message = (
            f"Warning: dictionary from {source_language} to {target_language} in "
            f"{dictionary_folder} already exists and is not empty"
        )
        LOGGER.warning(error_message)

    dataset_name = tbox.enrichment.dictionary.to_dataset(
        dictionary=dictionary,
        datasets_collection=tamr.datasets,
        target_language=target_language,
        source_language=source_language,
        create_dataset=True,
    )
    LOGGER.info(f"{dataset_name} created as a source dataset on Tamr")


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    parser.add_argument("--source", help="source language to translate from", required=True)
    parser.add_argument("--target", help="target language to translate to", required=True)

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
        dictionary_folder=CONFIG["translation"]["my_dictionary_folder"],
        source_language=args.source,
        target_language=args.target,
    )
