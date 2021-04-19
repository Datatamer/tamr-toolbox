"""Example script for bootstrapping a Tamr Categorization project with taxonomy categories"""
import argparse
import logging
import pandas as pd
import requests

from typing import Dict, Any

import tamr_toolbox as tbox
from tamr_toolbox.project.schema_mapping import schema

# Uncomment for bootstrapping categories at a specific taxonomy tier
# from tamr_toolbox.project.categorization import metrics


LOGGER = logging.getLogger(__name__)

CATEGORY_ATTRIBUTE_NAME = "Category Name"
FULL_PATH_LIST_NAME = "Full Path List"
FULL_PATH_STR_NAME = "Full Path"
COLUMN_NAMES = [FULL_PATH_STR_NAME, FULL_PATH_LIST_NAME]
PK_NAME = "Primary Key"


def main(*, instance_connection_info: Dict[str, Any], categorization_project_id: str) -> bool:
    """
    Bootstraps taxonomy in a categorization project

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        categorization_project_id: The id of the target categorization project

    Returns: Boolean indicating whether boostrap was successful or not

    Raises:
        TypeError: retrieved project is not a categorization project

    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the project
    project = tamr_client.projects.by_resource_id(categorization_project_id)
    LOGGER.info(f"Retrieved project with name: {project.name}")

    # Get the project taxonomy
    # NOTE: .taxonomy() function breaks on newer tamr instances SUP-5075
    try:
        project.taxonomy()
    except requests.exceptions.RequestException:
        no_taxonomy_error = f"Project {project.name} is not associated with any taxonomy yet."
        raise RuntimeError(no_taxonomy_error)
    LOGGER.info(f"Retrieved project taxonomy with name: {project.taxonomy().name}")

    # Bootstrap all available categories
    categories = project.taxonomy().categories()
    category_list = []
    for category in categories:
        category_list.append(category.path)
    # For bootstrapping at a specific tier = -1 (leaf), 1, 2, ... use the existing function
    # category_set = metrics._get_categories_at_tier(project=project, tier=1)
    # category_list = [category.split("|") for category in category_set]
    category_list.sort()

    # Create a dataset with taxonomy categories
    dataset_name = f"{project.unified_dataset().name}_taxonomy_bootstrap_dataset"
    try:
        project.client.datasets.by_name(dataset_name)
    except KeyError:
        # Dataset with `dataset_name` does not exist in Tamr. Proceed with dataset creation.
        pass
    else:
        dataset_exists_error = (
            f"A dataset with name {dataset_name} already exists. Try again after deleting the "
            "dataset."
        )
        LOGGER.error(dataset_exists_error)
        raise RuntimeError(dataset_exists_error)

    # Create a dictionary of full path between string and list (used as label path)
    taxonomy_dict = {", ".join(category): category for category in category_list}

    # Create a dataframe
    df = pd.DataFrame(list(taxonomy_dict.items()), columns=COLUMN_NAMES)

    # Add category data and hash for tamr_id
    df[CATEGORY_ATTRIBUTE_NAME] = df[FULL_PATH_LIST_NAME].apply(lambda x: x[-1])
    df[PK_NAME] = df[FULL_PATH_STR_NAME].apply(lambda x: hash(x))
    df.drop(FULL_PATH_LIST_NAME, axis=1, inplace=True)

    # Create a dataset in Tamr
    taxonomy_dataset = project.client.datasets.create_from_dataframe(
        df, primary_key_name=PK_NAME, dataset_name=dataset_name
    )
    LOGGER.info(f"Created a dataset in Tamr with name: {taxonomy_dataset.name}")

    # Add the dataset into the project
    project.add_input_dataset(taxonomy_dataset)
    LOGGER.info(f"Added {taxonomy_dataset.name} to project {project.name}")

    # Create a unified attribute to map taxonomy categories
    unified_attribute_name = "Taxonomy Categories"
    assert schema.create_unified_attribute(project, unified_attribute_name=unified_attribute_name)

    # Set ML configuration for the newly created unified attribute
    assert schema.set_unified_attribute_configurations(
        project,
        unified_attribute_name=unified_attribute_name,
        similarity_function="COSINE",
        tokenizer="DEFAULT",
        attribute_role="",
        is_numeric=False,
    )
    LOGGER.info(f"Created a unified attribute {unified_attribute_name}")

    # Tamr_id management is needed for label insert
    all_tx = tbox.project.schema_mapping.transformations.get_all(project)
    new_input_tx = tbox.project.categorization.transformations.InputTransformation(
        "SELECT *, to_string(origin_entity_id) AS tamr_id;", [taxonomy_dataset]
    )
    all_tx.input_scope.insert(0, new_input_tx)
    tbox.project.schema_mapping.transformations.set_all(project, all_tx)

    LOGGER.info("Updating the unified dataset...")
    tbox.project.categorization.jobs.update_unified_dataset(project)

    # Prepare and post labels
    category_dict = {", ".join(category): category for category in category_list}
    labels_to_bootstrap = [
        {
            "action": "CREATE",
            "recordId": hash(key),
            "record": {"verified": {"category": {"path": path}, "reason": "Taxonomy bootstrap"}},
        }
        for key, path in category_dict.items()
    ]
    project.client.post(
        f"projects/{project.resource_id}/categorizations/labels:updateRecords",
        json=labels_to_bootstrap,
    ).successful()
    LOGGER.info(f"Created and inserted labels into {project.name}")

    # Apply feedback and update results
    tbox.project.categorization.jobs.apply_feedback_and_update_results(project)
    LOGGER.info("Successfully applied and updated the model")
    LOGGER.info(f"Completed bootstrapping taxonomy in project {project.name}")


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
