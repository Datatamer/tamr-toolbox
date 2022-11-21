"""Example script for bootstrapping a Tamr Categorization project with taxonomy categories"""
import argparse
import logging
import pandas as pd
import requests

from typing import Any, Dict, Optional
from tamr_unify_client.project.attribute_mapping.resource import AttributeMappingSpec

import tamr_toolbox as tbox
from tamr_toolbox.project.categorization import metrics

LOGGER = logging.getLogger(__name__)

# name of existing unified attribute to be compared against category names
UNIFIED_ATTRIBUTE_NAME = "description"


def main(
    *,
    instance_connection_info: Dict[str, Any],
    categorization_project_id: str,
    unified_attribute_name: str,
    category_tier: Optional[int] = None,
) -> None:
    """Bootstraps the model for a categorization projcets by adding the taxonomy as a separate
    source with training labels

    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        categorization_project_id: The id of the target categorization project
        unified_attribute_name: The unified attribute to map the category names onto
        category_tier: Which tier of the taxonomy to confine labels to. Use -1 for leaf nodes.
            If not passed, all categories at all tiers will be used.

    Returns:
        Boolean indicating whether boostrap was successful or not

    Raises:
        TypeError: retrieved project is not a categorization project
        ValueError: retrieved project does not have an attribute of the specified name
    """

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)

    # Retrieve the project
    project = tamr_client.projects.by_resource_id(categorization_project_id).as_categorization()
    LOGGER.info(f"Retrieved project with name: {project.name}")

    # Validate dataset and attribute names
    # Confirm the target unified attribute exists
    try:
        project.attributes.by_name(unified_attribute_name)
    except requests.exceptions.HTTPError:
        raise RuntimeError(f"Project {project.name} has no attribute {unified_attribute_name}.")

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

    # Proceed with dataset creation
    # Get the project taxonomy
    try:
        project.taxonomy()
    except requests.exceptions.RequestException:
        raise RuntimeError(f"Project {project.name} is not associated with any taxonomy yet.")
    LOGGER.info(f"Retrieved project taxonomy with name: {project.taxonomy().name}")

    # Bootstrap all available categories
    categories = project.taxonomy().categories()
    if category_tier is None:
        category_list = [category.path for category in categories]
    else:
        category_set = metrics._get_categories_at_tier(project=project, tier=category_tier)
        category_list = [category.split("|") for category in category_set]
        category_list.sort()

    # Create a dictionary of full path as a string to the leaf node name (used as label path)
    taxonomy_dict = {", ".join(category): category[-1] for category in category_list}

    # Create a dataframe
    df = pd.DataFrame(list(taxonomy_dict.items()), columns=["Category Path", "Category Name"])

    # Create a dataset in Tamr
    taxonomy_dataset = project.client.datasets.create_from_dataframe(
        df, primary_key_name="Category Path", dataset_name=dataset_name
    )
    LOGGER.info(f"Created a dataset in Tamr with name: {taxonomy_dataset.name}")

    # Add the dataset into the project
    project.add_input_dataset(taxonomy_dataset)
    LOGGER.info(f"Added {taxonomy_dataset.name} to project {project.name}")

    # Map category name attribute to new unified attribute
    attr_mapping_spec = (
        AttributeMappingSpec.new()
        .with_input_dataset_name(dataset_name)
        .with_input_attribute_name("Category Name")
        .with_unified_dataset_name(project.unified_dataset().name)
        .with_unified_attribute_name(unified_attribute_name)
    )
    project.attribute_mappings().create(attr_mapping_spec.to_dict())
    LOGGER.info(
        f"Created mapping from source attribute 'Category Name' to unified attribute "
        f"{unified_attribute_name}"
    )

    # Create transformation ensuring dataset tamr_id values match categorization path
    all_tx = tbox.project.schema_mapping.transformations.get_all(project)
    new_tx = (
        f"SELECT *, CASE WHEN origin_source_name = '{dataset_name}' THEN "
        f"concat(origin_source_name, '_', origin_entity_id) ELSE tamr_id END AS tamr_id;"
    )
    # Append so that it is applied after any other possibly conflicting transformations
    all_tx.unified_scope.append(new_tx)
    tbox.project.schema_mapping.transformations.set_all(project, all_tx)

    LOGGER.info("Updating the unified dataset...")
    tbox.project.categorization.jobs.update_unified_dataset(project)

    # Prepare and post labels
    labels_to_bootstrap = [
        {
            "action": "CREATE",
            "recordId": f"{dataset_name}_{key}",
            "record": {"verified": {"category": {"path": path}, "reason": "Taxonomy bootstrap"}},
        }
        for key, path in taxonomy_dict.items()
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
    tbox.utils.logger.enable_toolbox_logging(log_directory=CONFIG["logging_dir"])

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        categorization_project_id=CONFIG["projects"]["my_categorization_project"],
        unified_attribute_name=UNIFIED_ATTRIBUTE_NAME,
    )
