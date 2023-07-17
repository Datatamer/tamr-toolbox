"""An example script to validate address data from disk and save results on disk."""
import argparse
from dataclasses import fields
from typing import List

import pandas as pd

import tamr_toolbox as tbox
from tamr_toolbox.enrichment.enrichment_utils import join_clean_tuple


def main(
    googlemaps_api_key: str,
    mapping_folder: str,
    address_columns: List[str],
    path_to_csv_to_validate: str,
    path_to_validated_csv: str,
) -> None:
    """Validate data located on disk and save results to disk.

    Uses the address validation mapping data found in the mapping folder, adding to it any new
    lookups needed. Adds columns corresponding to `AddressValidationMapping` fields to the data
    read from the csv, and saves it at `path_to_validated_csv`.

    Args:
        googlemaps_api_key: API key for the Google Maps address validation API
        mapping_folder: path to the folder on disk where local validation data is saved
        address_columns: ordered list of address columns from the local csv file
        path_to_csv_to_validate: Path to the CSV file to validate
        path_to_validated_csv: path to the CSV file augmented with validation data
    """
    # Make Google Maps API client
    maps_client = tbox.enrichment.api_client.google_address_validate.get_maps_client(
        googlemaps_api_key
    )

    # Read CSV file from disk
    dataframe = pd.read_csv(path_to_csv_to_validate, dtype=object)

    # Load any existing validation data
    LOGGER.info("Starting address validation.")
    mapping = tbox.enrichment.address_mapping.load(addr_folder=mapping_folder)

    # Validate attributes
    tuples = tbox.enrichment.enrichment_utils.dataframe_to_tuples(
        dataframe=dataframe, columns_to_join=address_columns
    )

    LOGGER.info("Generated %s tuples; beginning API validation", len(tuples))
    mapping = tbox.enrichment.address_validation.from_list(
        all_addresses=list(set(tuples)),
        client=maps_client,
        dictionary=mapping,
        region_code="US",
        intermediate_save_every_n=100,
        intermediate_save_to_disk=True,
        intermediate_folder=mapping_folder,
    )

    # Save to disk new mapping with added validation data
    LOGGER.info("Saving updated address validation data to disk.")
    tbox.enrichment.address_mapping.save(addr_mapping=mapping, addr_folder=mapping_folder)

    # Augmenting dataframe in situ
    LOGGER.info("Augmenting dataframe with validation data")

    # Add empty columns for each entry from the AddressValidation Mapping
    for att in fields(tbox.enrichment.address_mapping.AddressValidationMapping):
        dataframe[att.name + "_from_address_validation"] = None

    dataframe["lookup_key"] = [join_clean_tuple(tup) for tup in tuples]

    for ind, row in dataframe.iterrows():
        for att in fields(tbox.enrichment.address_mapping.AddressValidationMapping):
            col_name = att.name + "_from_address_validation"
            dataframe.at[ind, col_name] = getattr(mapping[row.lookup_key], att.name)

    # Then save dataframe to disk
    dataframe.to_csv(path_to_validated_csv, index=False)


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
    main(
        googlemaps_api_key=CONFIG["address_validation"]["googlemaps_api_key"],
        mapping_folder=CONFIG["address_validation"]["address_mapping_folder"],
        address_columns=CONFIG["address_validation"]["address_columns"],
        path_to_csv_to_validate="/path/to/data.csv",
        path_to_validated_csv="/path/to/augmented_data.csv",
    )
