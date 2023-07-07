"""An example script to validate address data from Tamr and save results in Tamr"""
from typing import Dict, Any, List

import tamr_toolbox as tbox

import argparse


def main(
    *,
    instance_connection_info: Dict[str, Any],
    unified_dataset_id: str,
    addr_unified_dataset_columns: List[str],
    mapping_dataset_id: str,
) -> None:
    """
    Validate address data streamed from Tamr and save results on Tamr

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        unified_dataset_id: id of the Tamr unified dataset containing the data to validate
        addr_unified_dataset_columns: ordered list of columns in the unified dataset with address
          info
        mapping_dataset_id: id of the Tamr toolbox address validation mapping dataset

    Returns:

    """
    # make Tamr Client, make GoogleMaps client
    tamr = tbox.utils.client.create(**instance_connection_info)
    maps_client = tbox.enrichment.api_client.google_address_validate.get_maps_client()

    # get dataframe from Tamr unified dataset: best is to pass a delta dataset where
    # only unvalidated data is kept.
    # To do this setup a SM project connected to your current validated UD and filter to records
    # with null/expired values in the validation columns
    dataset = tamr.datasets.by_resource_id(unified_dataset_id)
    df = tbox.data_io.dataframe.from_dataset(
        dataset, columns=addr_unified_dataset_columns, flatten_delimiter=" | "
    )

    # Stream dictionary from Tamr. Dictionaries must match  Toolbox AddressValidationMapping class
    dictionary_dataset = tamr.datasets.by_resource_id(mapping_dataset_id)
    dictionary = tbox.enrichment.dictionary.from_dataset(dictionary_dataset)

    LOGGER.info("Starting address validation.")
    dictionary = tbox.enrichment.address_validation.from_list(
        all_addresses=df[column]
        .unique()
        .tolist(),  # TODO: make a utility function to get tuples from dataframe
        client=maps_client,
        dictionary=dictionary,
        region_code="US",
    )

    # update dictionary on Tamr
    dataset_name = tbox.enrichment.address_mapping.to_dataset(
        dictionary=dictionary, dataset=dictionary_dataset
    )
    LOGGER.info("Tamr dataset %s updated with new validation data", dataset_name)
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
        addr_unified_dataset_columns=[
            "address_line1",
            "address_line_2",
            "city",
            "state",
            "postal_code",
        ],
        mapping_dataset_id=CONFIG["datasets"]["my_dictionary"]["id"],
    )
