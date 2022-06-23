"""Tasks related to project name and displayName"""
import json
import logging
import requests
from typing import Union

from tamr_unify_client import Client

LOGGER = logging.getLogger(__name__)


def _get_original_project_name(tamr_client: Client, *, project_id: Union[str, int]) -> str:
    """Get project's original name (as opposed to the `displayName`, which can be changed by users)

    Args:
        tamr_client: a Tamr client
        project_id: Tamr project's numerical identifier

    Returns:
        the project's original name
    """

    url = f"/api/versioned/v1/projects/{project_id}/unifiedDataset/usage"
    try:
        resp = tamr_client.get(url).successful()
    except requests.exceptions.HTTPError as e:
        message = f"Unable to retrieve project data: {e}."
        LOGGER.error(message)
        raise RuntimeError(message)

    name = json.loads(resp.content)["usage"]["outputFromProjectSteps"][0]["projectName"]

    return name
