import logging
from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.project.resource import Project
from tamr_unify_client.client import Client
from typing import List, Optional, Dict
from collections import OrderedDict
from tamr_toolbox.models.project_type import ProjectType
import re

LOGGER = logging.getLogger(__name__)


def datasets(dataset: Dataset, *, include_dependencies_by_name: bool = False) -> List[Dataset]:
    """Returns a dataset's downstream datasets.

    Args:
        dataset: The target dataset.
        include_dependencies_by_name: Whether to include datasets based on name similarity.
            No dependencies will be found by name if the `dataset` is not an unified dataset
            either based on backened pipeline (if project still exists) or based on
            regex (dataset name has suffix 'unified_dataset').

    Returns:
        List of Dataset objects ordered by number of its downstream dependencies.
            Note that there can be bidirectional dependency so datasets with same number of
            dependencies can depend on each other.
    """
    downstream_datasets = _find_downstream_datasets(
        dataset.name,
        include_dependencies_by_name=include_dependencies_by_name,
        client=dataset.client,
    )
    # Remove the target dataset from the list
    downstream_datasets.pop(dataset.name)
    # Extract the list of the Dataset objects
    downstream_datasets = list(downstream_datasets.values())
    # sort list of downstream datasets by number of dependencies
    # because parent dataset always has greater number of dependencies than its child datasets
    # note that there can be bidirectional dependency so datasets with same number of
    # dependencies can depend on each other
    LOGGER.info("Sorting the downstream datasets by number of dependencies")
    sorted_datasets = sorted(downstream_datasets, key=lambda x: len(x.usage().dependencies))
    return sorted_datasets


def projects(dataset: Dataset, *, include_dependencies_by_name: bool = False) -> List[Project]:
    """Return list of downstream project_list for a dataset.

    Args:
        dataset: The target dataset.
        include_dependencies_by_name: Whether to include datasets based on name similarity.
            No dependencies will be found by name if the `dataset` is not an unified dataset
            either based on backened pipeline (if project still exists) or based on
            regex (dataset name has suffix 'unified_dataset').

    Returns:
        List of downstream project_list in order,
            including the project the target `dataset` is part of.

    """
    downstream_datasets = datasets(
        dataset, include_dependencies_by_name=include_dependencies_by_name
    )
    project_ids = []
    # downstream dataset list is ordered by number of dependency ascending
    for dependency in downstream_datasets:
        project_ids.extend(_find_associated_projects(dependency))
    # add project_list of the target dataset
    project_ids.extend(_find_associated_projects(dataset))

    # use Ordered Dict to keep the order (set doesn't keep the order)
    project_ids = OrderedDict().fromkeys(project_ids)
    LOGGER.debug(f"Downstream project ids found: {project_ids}")

    # Need to reverse to get the most upstream project first
    project_list = [
        dataset.client.projects.by_resource_id(i) for i in reversed(project_ids.keys())
    ]
    return project_list


def _find_downstream_datasets(
    dataset_name: str,
    *,
    client: Client,
    include_dependencies_by_name: bool = False,
    all_tamr_datasets: Optional[Dict[str, Dataset]] = None,
    all_unified_datasets: Optional[List[str]] = None,
    downstream_datasets: Optional[Dict[str, Dataset]] = None,
) -> Dict[str, Dataset]:
    """Returns a dataset's downstream datasets.

    Args:
        dataset_name: Name of the target dataset.
        client: Tamr client
        include_dependencies_by_name: Whether to include datasets based on name similarity.
        all_tamr_datasets: A dictionary of all datasets existing in Tamr by name.
        all_unified_datasets: A List of current unified datasets in Tamr.
        downstream_datasets: A dictionary of downstream datasets by name.
            This argument is not to be set by users, and only used internally.

    Returns:
        List of Dataset objects.

    """
    # initialize optional variables
    if not all_tamr_datasets:
        all_tamr_datasets = {dataset.name: dataset for dataset in client.datasets}
    if not all_unified_datasets:
        all_unified_datasets = _get_all_unified_datasets(client)
    if not downstream_datasets:
        downstream_datasets = {}

    # Initialize a set of datasets need to check further downstream
    # (if include dependencies by name)
    datasets_check = set()
    LOGGER.debug(f"Start checking dependencies of {dataset_name}")
    dataset = all_tamr_datasets[dataset_name]
    dataset_dependencies = dataset.usage().dependencies
    LOGGER.debug(
        f"Dataset {dataset.name} has {len(dataset_dependencies)} "
        f"datasets with dependency suggested by pipeline"
    )
    for d in dataset_dependencies:
        if include_dependencies_by_name and _is_unified_dataset(
            d.dataset_name, all_unified_datasets=all_unified_datasets
        ):
            # if a downstream dataset is unified dataset and want to include dependencies by name,
            # need to check it further downstream
            datasets_check.add(d.dataset_name)
        elif d.dataset_name not in downstream_datasets.keys():
            downstream_datasets[d.dataset_name] = d.dataset()

    # get dependencies suggested by name matching
    # if the target dataset is or used to be unified dataset
    # and whose prefix is the target dataset's name if the target is unified dataset
    if include_dependencies_by_name and _is_unified_dataset(
        dataset_name, all_unified_datasets=all_unified_datasets
    ):
        dependencies_suggested_by_name = set(
            [d for d in all_tamr_datasets.keys() if d.startswith(dataset_name)]
        )
        LOGGER.debug(
            f"Dataset {dataset_name} has {len(dependencies_suggested_by_name)} "
            f"datasets with dependency suggested by name"
        )
        datasets_check = datasets_check.union(dependencies_suggested_by_name)
        # Filter out datasets that have already been checked and the target dataset
        datasets_check = datasets_check.difference(downstream_datasets.keys()).difference(
            {dataset.name}
        )

    # add the target dataset to list of downstream datasets to avoid checking later
    downstream_datasets[dataset_name] = dataset

    # check further downstream for either dependencies suggested by name
    # or dependencies that used to or is unified dataset
    # note: dictionary `downstream_datasets` is passed by reference
    for d in datasets_check:
        _find_downstream_datasets(
            d,
            include_dependencies_by_name=include_dependencies_by_name,
            client=client,
            all_tamr_datasets=all_tamr_datasets,
            all_unified_datasets=all_unified_datasets,
            downstream_datasets=downstream_datasets,
        )

    return downstream_datasets


def _find_associated_projects(dataset: Dataset) -> List[str]:
    """Return list of project_list that the dataset is part of.

    Args:
        dataset: The target dataset.

    Returns:
        List of Project IDs that the target `dataset` is part of.

    """
    project_list = set(
        [step.project().resource_id for step in dataset.usage().usage.input_to_project_steps]
    )
    return list(project_list)


def _is_unified_dataset(
    dataset_name: str,
    *,
    check_regex: bool = True,
    regex=".*unified_dataset$",
    all_unified_datasets: List[str],
) -> bool:
    """Check if a dataset is or used to be unified dataset.

    Args:
        dataset_name: Name of the target dataset.
        check_regex: A Boolean to determine whether to check for regex matching if a dataset is
            not part of the pipeline (if its project has been deleted.) Default to True.
        regex: A string of regex to check. Default to '.*unified_dataset$' which is Tamr's current
            default unified dataset naming.
        all_unified_datasets: List of unified dataset names.

    Returns:
        A Boolean, if True, the target `dataset` is or used to be unified dataset.

    """
    # check project pipeline (only works if project has not been deleted)
    if dataset_name in all_unified_datasets:
        return True
    # check regex
    if check_regex:
        return re.search(regex, dataset_name) is not None
    return False


def _get_all_unified_datasets(client: Client) -> List[str]:
    """

    Args:
        client: Tamr client

    Returns:
        List of unified dataset names.

    """
    # Use Project Spec to minimize number of API calls
    unified_dataset_names = [
        project.spec().to_dict()["unifiedDatasetName"]
        for project in client.projects
        # GOLDEN_RECORDS project does not have unified dataset
        if ProjectType[project.type]
        in [
            ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS,
            ProjectType.DEDUP,
            ProjectType.CATEGORIZATION,
        ]
    ]
    return unified_dataset_names
