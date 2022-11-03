"""Tasks related to editing the taxonomy for a tamr categorization project"""
import logging

from tamr_unify_client import Client
import pandas as pd


LOGGER = logging.getLogger(__name__)


def get_taxonomy_as_dataframe(client: Client, project_id: str) -> pd.DataFrame:
    """
    Returns the taxonomy for a project given the project ID.

    Args:
        client: Tamr client connected to target instance
        project_id: ID of the categorization project

    Returns:
        Current taxonomy categories as a dataframe

    Raises:
        RuntimeError: if project is not a categorization project or if the taxonomy does not exist
    """
    response = client.get(f"projects/{project_id}/taxonomy/categories")
    # Check for bad or empty response:
    if not response.ok or (response.text == "[]"):
        empty_response_error = (
            f"There was no taxonomy found for project {project_id}. Check if the project_id is "
            f"correct and if there is an existing taxonomy."
        )
        LOGGER.error(empty_response_error)
        raise RuntimeError(empty_response_error)

    max_depth = 0
    categories = []
    for blob in response.json():
        category = blob["path"]
        max_depth = max(max_depth, len(category))
        categories.append(category)

    tiers = [f"T{i}" for i in range(1, max_depth + 1)]
    taxonomy_dict = {}
    for tier in tiers:
        taxonomy_dict[tier] = []
    for category in categories:
        for tier_index, tier_name in enumerate(tiers):
            node_name = None
            if len(category) > tier_index:
                node_name = category[tier_index]
            taxonomy_dict[tier_name].append(node_name)

    return pd.DataFrame(taxonomy_dict)
