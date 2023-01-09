"""
This script provides an example of how to make changes to the taxonomy of a categorization project
without losing the verified categories already provided. Suppose we had the following taxonomy:

root
└── Animal & Pet Supplies
    ├── Cat Supplies
    ├── Crocodile Supplies
    └── Dog Supplies

And we wished to edit this taxonomy to the following instead:

root
└── Supplies
    ├── Animal Supplies
    │   └── Crocodile Supplies
    └── Pet Supplies
        ├── Cat Supplies
        └── Dog Supplies

This example makes the changes by doing the following in order:
1. Creating a new node "Supplies".
2. Moving node "Animal & Pet Supplies" to the node "Supplies -> Pet Supplies"
3. Creating a new node "Supplies -> Animal Supplies"
3. Moving "Supplies -> Pet Supplies -> Crocodile Supplies" to "Supplies -> Animal Supplies ->
Crocodile Supplies"

For larger scale changes, the required paths can also be provided programmatically.

"""
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

    # Create a new node "Supplies":
    new_path = ["Supplies"]
    LOGGER.info(f"Creating new node {new_path}")
    tbox.project.categorization.taxonomy.create_node(
        tamr_client, categorization_project_id, new_path
    )

    # Move node "Animal & Pet Supplies" to the node "Supplies -> Pet Supplies":
    old_path = ["Animal & Pet Supplies"]
    new_path = ["Supplies", "Pet Supplies"]
    LOGGER.info(f"Moving node {old_path} to {new_path}")
    tbox.project.categorization.taxonomy.move_node(
        tamr_client, categorization_project_id, old_path, new_path
    )

    # Create a new node "Supplies -> Animal Supplies"
    new_path = ["Supplies", "Animal Supplies"]
    LOGGER.info(f"Creating node {new_path}")
    tbox.project.categorization.taxonomy.create_node(
        tamr_client, categorization_project_id, new_path
    )

    # Move "Supplies -> Pet Supplies -> Crocodile Supplies" to "Supplies -> Animal Supplies ->
    # Crocodile Supplies":
    new_path = ["Supplies", "Animal Supplies", "Crocodile Supplies"]
    node_to_move = ["Supplies", "Pet Supplies", "Crocodile Supplies"]
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
