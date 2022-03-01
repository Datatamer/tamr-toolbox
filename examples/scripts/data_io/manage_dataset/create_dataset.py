"""Example script for creating a dataset"""
import argparse
from typing import Dict, Any, List

import tamr_toolbox as tbox
from tamr_unify_client.dataset.resource import DatasetSpec


def main(
    *,
    instance_connection_info: Dict[str, Any],
    dataset_name: str,
    attributes: List[str],
    primary_keys: List[str],
    description: str,
) -> None:
    """Creates a dataset in Tamr

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dataset_name: name of dataset
        attributes: list of attributes to create
        primary_key: primary key for dataset
        description: description of dataset
    """
    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)
    dataset_spec = (
        DatasetSpec.new()
        .with_name(dataset_name)
        .with_key_attribute_names(primary_keys)
        .with_description(description)
    )

    LOGGER.info(f"Creating dataset: {dataset_name}")

    tbox.data_io.manage_dataset.create(
        tamr=tamr_client, dataset_spec=dataset_spec, attributes=attributes,
    )


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config,
        default_path_to_file="examples/resources/conf/create_dataset.config.yaml",
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        dataset_name=CONFIG["datasets"]["my_source_dataset"]["name"],
        attributes=CONFIG["datasets"]["my_source_dataset"]["attributes"],
        primary_keys=CONFIG["datasets"]["my_source_dataset"]["primary_key"],
        description=CONFIG["datasets"]["my_source_dataset"]["description"],
    )
