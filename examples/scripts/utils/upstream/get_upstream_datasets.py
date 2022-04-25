"""Retrieve upstream datasets from a specified dataset."""

import argparse
from typing import List, Dict

import tamr_toolbox as tbox
from tamr_unify_client.dataset.resource import Dataset


def main(*, instance_connection_info: Dict[str, str], dataset_id: str) -> List[Dataset]:
    """Retrieve upstream datasets from a specified dataset.

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dataset_id: the dataset_id of the dataset for which upstream
        datasets are being retrieved

    Returns:
        List of upstream datasets

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    dataset = tamr_client.dataset.by_resource_id(dataset_id)

    # Retrieve upstream projects
    LOGGER.info(f"Retrieving upstream datasets for dataset: {dataset}")

    upstream_datasets = tbox.utils.upstream.projects(dataset)

    if upstream_datasets:
        LOGGER.info(
            f"The following upstream datasets were retrieved successfully {upstream_datasets}."
        )

    else:
        LOGGER.info(f"No upstream datasets found for dataset {dataset}.")

    return upstream_datasets


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/conf/dataset.config.yaml",
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        dataset_id=CONFIG["datasets"]["my_categorization_project_dataset"]["id"],
    )
