"""Example script for creation and editing of transformations of a Tamr project"""
from typing import Dict, Any

import tamr_toolbox as tbox
import argparse


def main(*, instance_connection_info: Dict[str, Any], project_id: str) -> None:
    """Edits transformations to a project

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        project_id: The id of the target project

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the project
    my_project = tamr_client.projects.by_resource_id(project_id)

    # Retrieve the transformations of a project
    LOGGER.info(f"Updating transformations for {my_project.name}")
    all_tx = tbox.project.schema_mapping.transformations.get_all(my_project)
    # this can also be called as tbox.project.mastering.transformations.get_all
    # this can also be called as tbox.project.categorization.transformations.get_all

    # View the transformations
    LOGGER.debug(all_tx)
    # Example print output
    """
    Transformations(
      input_scope=[
        InputTransformation(
          transformation='select *, lower(to_string(first_name)) as "first_name"',
          datasets=[
            tamr_unify_client.dataset.resource.Dataset(
              relative_id='datasets/3', name='people_tiny.csv', version='2')
              ]
          )
        ],
      unified_scope=[
        'SELECT *, concat(to_string(first_name), \' \', to_string(last_name)) as "full_name";',
        '//Just a comment',
        '//Example comment\nSELECT *, filter(is_not_empty, all_names) AS all_names;'
        ]
      )
    """

    # We edit the object all_tx locally. Once it is in the final desired state we post it to Tamr

    # Delete the second to last transformation on the unified dataset
    all_tx.unified_scope.pop(-2)

    # Append a transformation on the unified dataset
    all_tx.unified_scope.append("SELECT *, lower(to_string(last_name)) as last_name;")

    # Insert a transformation as the second transformation on the unified dataset
    all_tx.unified_scope.insert(1, "SELECT *, to_int(ssn) as ssn;")

    # Add a transformation as the first input transformation
    employee_dataset = my_project.input_datasets().by_name("employees_tiny.csv")
    new_input_tx = tbox.project.schema_mapping.transformations.InputTransformation(
        "SELECT *, to_int(ssn) as ssn;", [employee_dataset]
    )
    # this can also be called as tbox.project.mastering.transformations.InputTransformation
    # this can also be called as tbox.project.categorization.transformations.InputTransformation
    all_tx.input_scope.insert(0, new_input_tx)

    # View the transformations
    LOGGER.debug(all_tx)
    # Example print output
    """
    Transformations(
      input_scope=[
        InputTransformation(
          transformation='SELECT *, to_int(ssn) as ssn;',
          datasets=[
            tamr_unify_client.dataset.resource.Dataset(
              relative_id='datasets/64', name='employees_tiny.csv', version='162')
              ]
          ),
        InputTransformation(
          transformation='select *, lower(to_string(first_name)) as "first_name"',
          datasets=[
            tamr_unify_client.dataset.resource.Dataset(
              relative_id='datasets/3', name='people_tiny.csv', version='2')
              ]
          )
        ],
      unified_scope=[
        'SELECT *, concat(to_string(first_name), \' \', to_string(last_name)) as "full_name";',
        'SELECT *, to_int(ssn) as ssn;',
        '//Example comment\nSELECT *, filter(is_not_empty, all_names) AS all_names;',
        'SELECT *, lower(to_string(last_name)) as last_name;'
        ]
      )
    """

    # Set the transformations on your Tamr project with the updated transformations
    tbox.project.schema_mapping.transformations.set_all(my_project, all_tx)
    # this can also be called as tbox.project.mastering.transformations.set_all
    # this can also be called as tbox.project.categorization.transformations.set_all

    LOGGER.info(f"Completed updating transformations for {my_project.name}")


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
    # Direct the loggers for tamr-toolbox and tamr-unify-client to the same file
    tbox.utils.logger.enable_package_logging("tamr_toolbox", log_directory=CONFIG["logging_dir"])
    tbox.utils.logger.enable_package_logging(
        "tamr_unify_client", log_directory=CONFIG["logging_dir"]
    )

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        project_id=CONFIG["projects"]["my_schema_mapping_project"],
    )
