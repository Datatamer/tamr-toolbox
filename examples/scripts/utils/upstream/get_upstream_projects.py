"""Retrieve upstream projects from a specified project and update the entire chain."""

import argparse
from typing import List, Dict

import tamr_toolbox as tbox
from tamr_unify_client.operation import Operation


def main(*, instance_connection_info: Dict[str, str], project_id: str) -> List[Operation]:
    """Retrieve upstream projects from a specified project and update the entire chain.

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        project_id: the project_id of the project for which upstream
        projects are being retrieved

    Returns:
        List of operations run

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    project = tamr_client.projects.by_resource_id(project_id)

    # Retrieve upstream projects
    LOGGER.info(f"Retrieving upstream projects for project: {project}")

    upstream_projects = tbox.utils.upstream.projects(project)

    operations = []
    if upstream_projects:
        LOGGER.info(
            f"The following upstream projects were retrieved successfully {upstream_projects}"
        )

        # Update all projects in a chained workflow and return the operations that were carried out
        operations = tbox.workflow.jobs.run(upstream_projects)
    else:
        LOGGER.info(f"No upstream projects found for project {project}")

    # Now that all upstream project have been run, run the target project
    # and add it's operations to the list
    operations.extend(tbox.workflow.jobs.run([project]))

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
        project_id=CONFIG["projects"]["my_golden_records_project"],
    )
