"""Example script for running a Tamr Categorization project without model training"""
import argparse
from typing import Dict, Any

import tamr_toolbox as tbox


def main(*, instance_connection_info: Dict[str, Any], categorization_project_id: str) -> None:
    """Loads the taxonomy of a categorization project and creates a node, renames it, deletes it
    and moves a node to another location.

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        categorization_project_id: The id of the target categorization project

    Returns: None

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the taxonomy of the project as a dataframe
    taxonomy_df = tbox.project.categorization.taxonomy.get_taxonomy_as_dataframe(
        tamr_client, categorization_project_id
    )
    all_paths = taxonomy_df.agg(lambda x: list(x.dropna()), axis=1).tolist()

    # Create a new node "Test Node" under the first path:
    new_path = all_paths[0].append("Test Node")
    LOGGER.info(f"Creating new node {new_path}")
    tbox.project.categorization.taxonomy.create_node(
        tamr_client, categorization_project_id, new_path
    )

    # Rename the newly created node to "Renamed Test Node":
    new_name = "Renamed Test Node"
    LOGGER.info(f"Renaming node {new_path} to {new_name}")
    tbox.project.categorization.taxonomy.rename_node(
        tamr_client, categorization_project_id, new_name, new_path
    )

    # Delete the last node in the taxonomy:
    delete_path = all_paths[-1]
    LOGGER.info(f"Deleting node {delete_path} from the taxonomy")
    tbox.project.categorization.taxonomy.delete_node(
        tamr_client, categorization_project_id, delete_path
    )

    # Move the new current last node to be under the newly created "Renamed Test Node":
    new_path = all_paths[0].append(new_name)
    node_to_move = all_paths[-1]
    LOGGER.info(f"Moving node {node_to_move} to be under {new_path}")
    tbox.project.categorization.taxonomy.move_node(
        tamr_client, categorization_project_id, node_to_move, new_path
    )

    return


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
        categorization_project_id=CONFIG["projects"]["my_categorization_project"],
    )
