"""Example script for running a pipeline consisting of multiple Tamr projects"""
import argparse
from typing import List, Dict

from tamr_unify_client.operation import Operation

import tamr_toolbox as tbox


def main(*, instance_connection_info: Dict[str, str], project_ids: List[str]) -> List[Operation]:
    """Runs the continuous steps of a multiple projects of any type

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        project_ids: The ids of the target projects to run in order

    Returns: List of jobs run

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the projects
    my_projects = [tamr_client.projects.by_resource_id(p_id) for p_id in project_ids]
    LOGGER.info(f"About to run projects: {[p.name for p in my_projects]}")

    # runs the projects specifying to wait 30 seconds between jobs
    operations = tbox.workflow.jobs.run(
        my_projects, run_apply_feedback=False, run_estimate_pair_counts=False, sleep_interval=30
    )

    LOGGER.info(f"Tasks for {[p.name for p in my_projects]} complete")

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
        project_ids=[
            CONFIG["projects"]["my_schema_mapping_project"],
            CONFIG["projects"]["my_categorization_project"],
            CONFIG["projects"]["my_mastering_project"],
            CONFIG["projects"]["my_golden_records_project"],
        ],
    )
