"""Tasks related to transformations with Tamr projects"""
from typing import List
from dataclasses import dataclass, field
from requests import Response
import logging

from tamr_unify_client import response, Client
from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.project.resource import Project

from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.utils import version

LOGGER = logging.getLogger(__name__)

TX_RELEASE_VERSION = "2020.009.0"


@dataclass()
class InputTransformation:
    """A transformation scoped to input datasets

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        transformation: The text of a transformations script
        datasets: The list of input datasets that the script should be applied to
    """

    transformation: str
    datasets: List[Dataset] = field(default_factory=list)


@dataclass()
class TransformationGroup:
    """A group of input transformations and unified transformations

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        input_scope: A list of transformation to apply to input datasets
        unified_scope: A list of transformation scripts to apply to the unified dataset
    """

    input_scope: List[InputTransformation] = field(default_factory=list)
    unified_scope: List[str] = field(default_factory=list)


def _input_transformation_from_json(tamr_client: Client, data: JsonDict) -> InputTransformation:
    """Make input transformation from JSON data (deserialize)

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        tamr_client: A Tamr client
        data: Input scoped transformation JSON data from Tamr server

    Returns:
        A single input transformation
    """
    # "datasets" is a dictionary with 3 keys: name, datasetId and relativeDatasetId
    # The datasetId is formatted like: "datasetId": "unify://unified-data/v1/datasets/3"
    # If we split that string on / , the last value will be the dataset resource id
    dataset_resource_ids = [d["datasetId"].split("/")[-1] for d in data["datasets"]]
    datasets = [tamr_client.datasets.by_resource_id(d_id) for d_id in dataset_resource_ids]
    # "transformation" is the text of the transformation script
    return InputTransformation(transformation=data["transformation"], datasets=datasets)


def _from_json(tamr_client: Client, data: JsonDict) -> TransformationGroup:
    """Make transformations from JSON data (deserialize)

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        tamr_client: A Tamr client
        data: Transformation JSON data from Tamr server

    Returns:
        All input transformations and unified transformations
    """
    return TransformationGroup(
        # "unified" is a list of transformations for the unified dataset
        unified_scope=data["unified"],
        # "parameterized" is a list of dictionaries representing input transformations
        input_scope=[
            _input_transformation_from_json(tamr_client, tx) for tx in data["parameterized"]
        ],
    )


def _input_transformation_to_json(tx: InputTransformation) -> JsonDict:
    """Convert input transformations to JSON data (serialize)

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        tx: Input transformation to convert

    Returns:
        JSON representation of a single input transformation
    """
    # datasetId omitted, only one of "datasetId" or "relativeDatasetId" is required
    dataset_json = [
        {"name": d.name, "relativeDatasetId": f"datasets/{d.resource_id}"} for d in tx.datasets
    ]

    return {"datasets": dataset_json, "transformation": tx.transformation}


def _to_json(tx: TransformationGroup) -> JsonDict:
    """Convert transformations to JSON data (serialize)

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        tx: Transformations to convert

    Returns:
        JSON representation of all input transformations and unified transformations

    """
    return {
        "parameterized": [_input_transformation_to_json(t) for t in tx.input_scope],
        "unified": tx.unified_scope,
    }


def get_all(project: Project) -> TransformationGroup:
    """Get the transformations of a Project

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        project: Project containing transformations

    Returns:
        All input transformations and unified transformations of a project
    """
    version.enforce_after_or_equal(project.client, compare_version=TX_RELEASE_VERSION)

    LOGGER.info(f"Getting transformations from project {project}")
    r = project.client.get(f"{project.api_path}/transformations")
    response.successful(r)
    return _from_json(project.client, r.json())


def set_all(project: Project, tx: TransformationGroup, *, allow_overwrite=True) -> Response:
    """Set the transformations of a Project

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        project: Project to place transformations within
        tx: Transformations to put into project
        allow_overwrite: Whether existing transformations can be overwritten

    Returns:
        Response object created when transformations of a project are replaced

    Raises:
        RuntimeError: if `allow_overwrite` is set to False but transformations already exists
            in `project`
        ValueError: if provided `tx` are invalid
    """
    version.enforce_after_or_equal(project.client, compare_version=TX_RELEASE_VERSION)

    if not allow_overwrite:
        # Fail if any transformations already exist
        current_tx = get_all(project)
        if (len(current_tx.unified_scope) + len(current_tx.input_scope)) != 0:
            error_message = (
                f"Cannot set transformations in project {project} because "
                f"transformations already exist in that project and allow_overwrite "
                f"is set to False.)"
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message)

    LOGGER.info(f"Replaces transformations in project {project}")
    body = _to_json(tx)
    r = project.client.put(f"{project.api_path}/transformations", json=body)

    # Raise a specific exception for invalid transformation input
    if r.status_code == 400:
        error_message = (
            f"Failed to set transformations in project {project} due to invalid "
            f"transformations: {r.json()['message']}"
        )
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # Other issue with set are caught here
    return response.successful(r)


def get_all_unified(project: Project) -> List[str]:
    """Get the unified transformations of a Project

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        project: Project containing transformations

    Returns:
        All unified transformations of a project
    """
    return get_all(project).unified_scope


def set_all_unified(project: Project, tx: List[str], *, allow_overwrite=True) -> Response:
    """Set the unified transformations of a Project.
    Any input transformations will not be altered

    Version:
        Requires Tamr 2020.009.0 or later

    Args:
        project: Project to place transformations within
        tx: Unified transformations to put into project
        allow_overwrite: Whether existing unified transformations can be overwritten

    Returns:
        Response object created when transformations of a project are replaced

    Raises:
        RuntimeError: if `allow_overwrite` is set to False but transformations already exists
            in `project`
    """
    current_tx = get_all(project)

    if not allow_overwrite:
        # Fail if any unified transformations already exist
        if len(current_tx.unified_scope) != 0:
            error_message = (
                f"Cannot set unified transformations in project {project} because "
                f"unified transformations already exist in that project and "
                f"allow_overwrite is set to False.)"
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message)

    new_tx = TransformationGroup(unified_scope=tx, input_scope=current_tx.input_scope)

    # Here allow_overwrite is true, as input transformations are allowed to be present
    return set_all(project, new_tx, allow_overwrite=True)
