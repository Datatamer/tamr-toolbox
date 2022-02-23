"""Example script for exporting a dataset from Tamr as a CSV"""
import argparse
from typing import Dict, Any

import tamr_toolbox as tbox


def main(
    *, instance_connection_info: Dict[str, Any], dataset_id: str, export_file_path: str
) -> None:
    """Exports a csv from a dataset

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dataset_id: The id of the dataset to export
        export_file_path: Path to write the csv file to
    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    dataset = tamr_client.datasets.by_resource_id(dataset_id)

    # Export the default using default settings
    tbox.data_io.csv.from_dataset(dataset, export_file_path=export_file_path)

    # Export after adjusting some of the default parameters including csv delimiter,
    # flattening delimiter for recording multi-values, limiting and ordering the columns, limiting
    # the number of rows, adjusting the buffer size that determines at what interval records are
    # written to disk, and renaming the "last_name" column to "family_name"
    tbox.data_io.csv.from_dataset(
        dataset,
        export_file_path,
        csv_delimiter=";",
        flatten_delimiter="#",
        columns=["tamr_id", "last_name", "first_name"],
        nrows=1000,
        buffer_size=100,
        column_name_dict={"last_name": "family_name"},
    )

    LOGGER.info("Writing CSV is complete")


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
        dataset_id=CONFIG["datasets"]["my_mastering_project_dataset"]["id"],
        export_file_path=CONFIG["datasets"]["my_mastering_project_dataset"]["export_file_path"],
    )
