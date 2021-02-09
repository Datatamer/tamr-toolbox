"""Example script for running a Mastering project step-by-step including model training"""
import argparse
from typing import List, Dict, Any

from tamr_unify_client.operation import Operation

import tamr_toolbox as tbox


def main(
    *, instance_connection_info: Dict[str, Any], mastering_project_id: str
) -> List[Operation]:
    """Runs all steps of a mastering project

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

    # Run all steps of a project, step-by-step
    LOGGER.info(f"About to run project with training: {my_project.name}")

    op_list1 = tbox.project.mastering.jobs.update_unified_dataset(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list1]}")

    op_list2 = tbox.project.mastering.jobs.estimate_pair_counts(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list2]}")

    op_list3 = tbox.project.mastering.jobs.generate_pairs(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list3]}")

    op_list4 = tbox.project.mastering.jobs.apply_feedback_and_update_results(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list4]}")

    op_list5 = tbox.project.mastering.jobs.publish_clusters(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list5]}")

    LOGGER.info(f"All tasks for {my_project.name} complete")

    # Each step returns a list of operations
    # We return a combined list of all operation run in the script, in the order that they were run
    return [*op_list1, *op_list2, *op_list3, *op_list4, *op_list5]


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
