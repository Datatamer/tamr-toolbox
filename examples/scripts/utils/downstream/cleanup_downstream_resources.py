"""
Example script for checking and cleaning up downstream datasets.
NOTE: USE AT YOUR OWN RISK. Deleting datasets can sometimes break the system
and should only be done when necessary. Always take backup before you perform the cleanup.
"""
import tamr_toolbox as tbox
from requests.exceptions import HTTPError
from typing import Dict, Any
import argparse


def main(*, instance_connection_info: Dict[str, Any], dataset_name_to_cleanup: str) -> bool:
    """Check and clean up a Tamr dataset and its downstream datasets if possible.

        Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        dataset_name_to_cleanup: name of target dataset to clean up

    Returns:
        Whether the cleanup of the dataset and its downstream datasets has been done successfully.

    """
    # Create Tamr Client
    tamr = tbox.utils.client.create(**instance_connection_info)
    my_dataset = tamr.datasets.by_name(dataset_name_to_cleanup)

    # Get the list of downstream datasets by pipeline dependency
    # Note: If you want to include dependencies suggested by name matching,
    # (some datasets are part of a project but not registered in the pipeline)
    # set `include_dependencies_by_name` to True but check
    # all downstream datasets before deleting to make sure you do not delete
    # any unexpected datasets
    downstream_datasets = tbox.utils.downstream.datasets(my_dataset)
    downstream_projects = tbox.utils.downstream.projects(my_dataset)

    # Clean up if there is no downstream projects
    if len(downstream_projects) == 0:
        # Delete all downstream datasets
        for dataset in downstream_datasets:
            try:
                dataset.delete(cascade=True)
                LOGGER.info(f"Deleted dataset {dataset.name}")
            except HTTPError as e:
                if e.response.status_code == 404:
                    LOGGER.info(f"Dataset {dataset.name} has already been deleted.")
                else:
                    LOGGER.error(f"Error occurred when trying to delete {dataset.name}: {e}")
                    raise Exception(e)
        # Delete the target dataset
        my_dataset.delete()
        LOGGER.info(f"Deleted datset {my_dataset.name}")
        return True
    else:
        LOGGER.error(
            f"The dataset '{my_dataset.name}' cannot be deleted because the"
            f"following downstream projects depend on it or its dependencies: "
            f"{[p.name for p in downstream_projects]}"
        )
        return False


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/conf/project.config.yaml",
    )
    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])
    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        dataset_name_to_cleanup=CONFIG["datasets"]["dataset_to_clean_up"],
    )
