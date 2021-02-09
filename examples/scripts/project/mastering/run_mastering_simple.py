"""Example script for running a Tamr Mastering project without model training"""
import argparse
from typing import List, Dict, Any

from tamr_unify_client.operation import Operation

import tamr_toolbox as tbox


def main(
    *, instance_connection_info: Dict[str, Any], mastering_project_id: str
) -> List[Operation]:
    """Runs the continuous steps (no training) of a mastering project

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        mastering_project_id: The id of the target mastering project

    Returns: List of jobs run

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the project
    my_project = tamr_client.projects.by_resource_id(mastering_project_id)
    my_project = my_project.as_mastering()

    # Run the typical steps of a project
    LOGGER.info(f"About to run project: {my_project.name}")
    operations = tbox.project.mastering.jobs.run(
        my_project, run_apply_feedback=False, run_estimate_pair_counts=False
    )
    LOGGER.info(f"Tasks for {my_project.name} complete")

    return operations


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    args = parser.parse_args()

    # Load the configuration from the file path provided or the default file path specified
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=args.config, default_path_to_file="/path/to/my/conf/project.config.yaml"
    )

    # Use the configuration to create a global logger
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        mastering_project_id=CONFIG["projects"]["my_mastering_project"],
    )
