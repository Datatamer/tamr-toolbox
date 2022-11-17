"""Tasks related to editing the taxonomy for a tamr categorization project"""
import logging

from tamr_unify_client import Client
import pandas as pd
import json

LOGGER = logging.getLogger(__name__)


def get_children_nodes(all_categories: list, target_node: str):
    """
    Gets all the children nodes for a given node ID from the full taxonomy

    Args:
        all_categories: List of all categories of the taxonomy returned by categories API
        target_node: Full internal ID of target node to get children for

    Returns: List of children nodes including non-leaf nodes under the given target node
    """
    first_pass_children = [cat for cat in all_categories if cat["parent"] == target_node]
    if len(first_pass_children) == 0:
        children_nodes = []
    else:
        children_nodes = first_pass_children
        while True:
            new_children_all = []
            for node in first_pass_children:
                new_children = [cat for cat in all_categories if cat["parent"] == node["id"]]
                if len(new_children) > 0:
                    children_nodes.extend(new_children)
                    new_children_all.extend(new_children)
            if len(new_children_all) == 0:
                break
            else:
                first_pass_children = new_children_all

    return children_nodes


def delete_node(client: Client, project_id: str, path: list, force_delete: bool = False):
    """
    Deletes a node from a taxonomy.

    Args:
        client: Tamr client connected to target instance
        project_id: ID of the categorization project
        path: Full path of the node to be deleted
        force_delete: Optional flag. Default is false. If true, deletes even if there are still
        records assigned to that category. If false, the operation fails with an error.

    Returns: None
    """
    # Get all categories to extract category ID:
    cat_response = client.get(f"projects/{project_id}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == path][0]
    target_cat_full_id = target_cat["id"]
    target_cat_id = target_cat_full_id.split("/")[-1]

    # Check if there are any children nodes to the target node:
    children_nodes = get_children_nodes(all_cats, target_cat_full_id)
    all_children_ids = [cat["id"] for cat in children_nodes]
    all_children_ids.append(target_cat_full_id)

    # Check if there are any records verified to the category to be deleted:
    verified_cats_response = client.get(f"projects/{project_id}/categorizations/labels/records")
    node_records = []
    for line in verified_cats_response.iter_lines():
        record = json.loads(line)
        if record["verified"]["category"]["categoryId"] in all_children_ids:
            node_records.append(record)

    if len(node_records) == 0:
        LOGGER.info(f"No records found for node {path} or any children nodes. Deleting node.")
        client.delete(f"projects/{project_id}/taxonomy/categories/{target_cat_id}")
    else:
        LOGGER.info(f"There are verified records under node {path}. Deleting the node without "
                    f"moving these records will result in loss of work. If you wish to proceed "
                    f"set force_delete flag to True.")
        if force_delete:
            LOGGER.info(f"Force Delete is set to true. Deleting node {path}")
            client.delete(f"projects/{project_id}/taxonomy/categories/{target_cat_id}")
        else:
            delete_node_error = f"Force delete is set to false. Exiting without deletion."
            LOGGER.error(delete_node_error)
            raise RuntimeError(delete_node_error)
    return


def rename_node(client: Client, project_id: str, new_name: str, path: list):
    """
    Renames an existing node in the taxonomy.

    Args:
        client: Tamr client connected to target instance
        project_id: ID of the categorization project
        new_name: New name to assign to the leaf node
        path: Full path of the existing leaf node to rename

    Returns: None
    """
    body = {
        "path": path,
        "name": new_name
    }
    # Get all categories to extract category ID:
    cat_response = client.get(f"projects/{project_id}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == path][0]
    target_cat_id = target_cat["id"].split("/")[-1]
    LOGGER.info(f"Renaming category id {target_cat_id} in project {project_id} to {new_name}")
    response = client.put(f"projects/{project_id}/taxonomy/categories/{target_cat_id}",
                          json=body)
    # Log an error if the operation failed:
    if not response.ok:
        content = json.loads(response.content)
        rename_node_error = f"Renaming node {target_cat_id} failed with message " \
                            f"{content['message']}"
        LOGGER.error(rename_node_error)
        raise RuntimeError(rename_node_error)
    return


def create_node(client: Client, project_id: str, path: list):
    """
    Creates a category with the specified path in the project taxonomy.

    Args:
        client: Tamr client connected to target instance
        project_id: ID of the categorization project
        path: Full path of the new category to be added

    Returns: None
    """
    body = {
        "name": path[-1],
        "path": path
    }
    LOGGER.info(f"Creating new category {path[-1]} in project {project_id}")
    response = client.post(f"projects/{project_id}/taxonomy/categories", json=body)
    # Log an error if the operation failed:
    if not response.ok:
        content = json.loads(response.content)
        create_node_error = f"Creating node {path[-1]} failed with message {content['message']}"
        LOGGER.error(create_node_error)
        raise RuntimeError(create_node_error)
    return


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
    LOGGER.info(f"Retrieving taxonomy for project ID {project_id}")
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
