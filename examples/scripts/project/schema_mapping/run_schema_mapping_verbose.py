"""Example script for running a Schema Mapping project step-by-step"""
import argparse
from typing import List, Dict, Any

from tamr_unify_client.operation import Operation

import tamr_toolbox as tbox


def main(
    *, instance_connection_info: Dict[str, Any], schema_mapping_project_id: str
) -> List[Operation]:
    """Runs all steps of a schema mapping project

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        schema_mapping_project_id: The id of the target schema mapping project

    Returns: List of jobs run

    """
    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the project
    my_project = tamr_client.projects.by_resource_id(schema_mapping_project_id)

    # Run all steps of a project, step-by-step
    LOGGER.info(f"About to run project: {my_project.name}")

    op_list1 = tbox.project.schema_mapping.jobs.update_unified_dataset(my_project)
    LOGGER.info(f"Completed: {[op.description for op in op_list1]}")

    LOGGER.info(f"All tasks for {my_project.name} complete")

    # Each step returns a list of operations
    # We return a combined list of all operation run in the script, in the order that they were run
    return [*op_list1]


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
        schema_mapping_project_id=CONFIG["projects"]["my_schema_mapping_project"],
    )
