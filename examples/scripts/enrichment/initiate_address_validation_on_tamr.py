"""An example script to create an address validation mapping on Tamr."""
import argparse
from typing import Dict, Any

import tamr_toolbox as tbox


def main(
    *, instance_connection_info: Dict[str, Any], existing_mapping_folder: str, dataset_name: str
) -> None:
    """Create a toolbox address validation mapping dataset on Tamr.

    If a mapping is found in the `existing_mapping_folder`, it will be loaded to Tamr; otherwise
    an empty dataset is created in Tamr (no file will be created on the local filesystem).

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        existing_mapping_folder: Path to the folder on disk for existing validation data
        dataset_name: name for the new address validation dataset in Tamr
    """
    # Connect to tamr
    tamr = tbox.utils.client.create(**instance_connection_info)

    LOGGER.info("Initializing address validation mapping dataset on Tamr.")
    # Load existing data. If existing data is saved under another name than the default
    # "address_validation_mapping.json", pass the filename to the `load` function here
    mapping = tbox.enrichment.address_mapping.load(addr_folder=existing_mapping_folder)

    if len(mapping) > 0:
        LOGGER.warning(
            "Alert: address validation mapping in %s already exists and is not empty",
            existing_mapping_folder,
        )

    dataset_name = tbox.enrichment.address_mapping.to_dataset(
        addr_mapping=mapping,
        datasets_collection=tamr.datasets,
        create_dataset=True,
        dataset_name=dataset_name,
    )
    LOGGER.info("Dataset %s created as a source dataset on Tamr", dataset_name)


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)

    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/address_validation.config.yaml"
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    # Use config name for `my_addr_validation_mapping` if supplied, otherwise use default
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        existing_mapping_folder=CONFIG["address_validation"]["address_mapping_folder"],
        dataset_name=CONFIG.get("datasets", dict())
        .get("my_addr_validation_mapping", dict())
        .get("name", "address_validation_mapping"),
    )
