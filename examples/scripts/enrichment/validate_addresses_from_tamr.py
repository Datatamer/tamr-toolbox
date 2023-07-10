"""An example script to validate address data from Tamr and save results in Tamr"""
import argparse
from datetime import timedelta
from typing import Any, Dict, List

import tamr_toolbox as tbox


def main(
    *,
    instance_connection_info: Dict[str, Any],
    dataset_id: str,
    dataset_addr_columns: List[str],
    mapping_dataset_id: str,
) -> None:
    """Validate address data streamed from Tamr and save results on Tamr.

    Note that this does not update the dataset corresponding to the input `dataset_id` -- it
    performs lookups based on data in that dataset, and updates the dataset corresponding to the
    `mapping_dataset_id` with the new data.

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dataset_id: id of the Tamr dataset containing the data to validate
        dataset_addr_columns: ordered list of columns in the unified dataset with address info
        mapping_dataset_id: id of the Tamr toolbox address validation mapping dataset

    Returns:

    """
    # Make Tamr Client
    tamr = tbox.utils.client.create(**instance_connection_info)

    # Get dataframe from Tamr dataset.
    # For large datasets, it is to preferable to use a delta dataset with only unvalidated/expired
    # data. To do this, set up a SM project connected to current validated dataset and filter to
    # records with null/expired values in the validation columns
    dataset = tamr.datasets.by_resource_id(dataset_id)
    dataframe = tbox.data_io.dataframe.from_dataset(
        dataset, columns=dataset_addr_columns, flatten_delimiter=" | "
    )

    # Stream address mapping data from Tamr -- must match Toolbox AddressValidationMapping class
    mapping_dataset = tamr.datasets.by_resource_id(mapping_dataset_id)
    mapping = tbox.enrichment.address_mapping.from_dataset(mapping_dataset)

    LOGGER.info("Starting address validation.")
    maps_client = tbox.enrichment.api_client.google_address_validate.get_maps_client()

    tuples = tbox.enrichment.enrichment_utils.dataframe_to_tuples(
        dataframe=dataframe, columns_to_join=dataset_addr_columns
    )

    # Update the `region_code` below to match the expected region of your dataset, or remove it if
    # no `region_code` can be inferred
    # Update the expiration date buffer depending on update frequency of your pipeline
    mapping = tbox.enrichment.address_validation.from_list(
        all_addresses=tuples,
        client=maps_client,
        dictionary=mapping,
        enable_usps_cass=False,
        region_code="US",
        expiration_date_buffer=timedelta(days=2),
    )

    # Update address validation mapping on Tamr
    dataset_name = tbox.enrichment.address_mapping.to_dataset(
        addr_mapping=mapping, dataset=mapping_dataset
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
        path_to_file=args.config,
        default_path_to_file="/path/to/my/conf/address_validation.config.yaml",
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        dataset_id=CONFIG["datasets"]["my_dataset_to_be_addr_validated"]["id"],
        dataset_addr_columns=CONFIG["datasets"]["my_dataset_to_be_addr_validated"][
            "address_columns"
        ],
        mapping_dataset_id=CONFIG["datasets"]["my_addr_validation_mapping"]["id"],
    )
