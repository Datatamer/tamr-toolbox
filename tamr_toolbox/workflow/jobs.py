"""Tasks related to running jobs for groups of Tamr projects"""
from typing import List, Optional
import logging

from tamr_unify_client.operation import Operation
from tamr_unify_client.project.resource import Project
from tamr_toolbox.models.project_type import ProjectType

from tamr_toolbox.project import (
    mastering,
    categorization,
    golden_records,
    schema_mapping,
)

LOGGER = logging.getLogger(__name__)


def run(
    project_list: List[Project],
    *,
    run_apply_feedback: bool = False,
    run_estimate_pair_counts: bool = False,
) -> List[Operation]:
    """Run multiple projects in order

    Args:
        project_list: A list of Tamr projects
        run_apply_feedback: Whether train should be called on the pair matching model
            or categorization model (based on project type)
        run_estimate_pair_counts: Whether an estimate pairs job should be run

    Returns:
        The operations that were run

    Raises:
        NotImplementedError: Raised if run() is called on an unsupported project type

    """
    operations = []
    for project in project_list:
        target_type = ProjectType[project.type]
        if target_type == ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS:
            operations.extend(schema_mapping.jobs.run(project))
        elif target_type == ProjectType.CATEGORIZATION:
            operations.extend(
                categorization.jobs.run(
                    project.as_categorization(), run_apply_feedback=run_apply_feedback
                )
            )
        elif target_type == ProjectType.DEDUP:
            operations.extend(
                mastering.jobs.run(
                    project.as_mastering(),
                    run_apply_feedback=run_apply_feedback,
                    run_estimate_pair_counts=run_estimate_pair_counts,
                )
            )
        elif target_type == ProjectType.GOLDEN_RECORDS:
            operations.extend(golden_records.jobs.run(project))
        else:
            error_msg = (
                f"Function run.continuous_operations not defined for project type {project.type}"
            )
            LOGGER.error(error_msg)
            raise NotImplementedError(error_msg)

    return operations


def _find_upstream_projects(
    project: Project,
    all_projects: Optional[List[Project]] = None,
    project_list: Optional[List[str]] = None,
    upstream_projects: Optional[List[str]] = None,
) -> List[Project]:
    """ Check for upstream projects that have source datasets that require
        updating
        Args:
            project: a tamr project
            track of projects that need to be checked for upstream dependencies
            all_projects: list of all projects that exist within Tamr
            project_list: keeps track of projects that have yet to be checked
            for upstream dependencies.
            upstream_projects: list to keep track of projects within a chain of projects
        Returns:
            upstream_projects: final list of tamr projects in a chained
            workflow
        """
    # Obtain the name of project to be updated and initiate the list of
    # projects (project_list) that are maintained within the check_for_upstream_project
    # function to check for upstream dependencies

    # Get project_id from project
    project_id = project.resource_id

    if not upstream_projects:
        upstream_projects = []
        project_list = [project_id]

        all_projects = {}

        for project in project.client.projects:
            all_projects[project.name] = project.resource_id

    for project_id in project_list:
        # Obtain resource id of project to be updated from the dictionary of
        # project details
        project = project.client.projects.by_resource_id(project_id)
        if project.resource_id not in upstream_projects:
            upstream_projects.append(project.resource_id)

        # Remove the current project from the list tracking projects that
        # need to be reviewed for upstream projects/dependencies
        project_list.remove(project_id)

        project_source_datasets = project.client.get(
            f"/api/versioned/v1/projects/{project.resource_id}/inputDatasets"
        ).json()
        relative_ids = [dataset["relativeId"] for dataset in project_source_datasets]

        # Check which upstream projects the upstream datasets derive from
        for relative_id in relative_ids:
            dataset_details = project.client.datasets.by_relative_id(relative_id)

            # If these projects are linked to further datasets upstream,
            # keep track of these upstream projects in project_list
            upstream_datasets = dataset_details.usage().usage.output_from_project_steps
            if upstream_datasets:
                for upstream_dataset in upstream_datasets:
                    upstream_project = upstream_dataset.project_name
                    project_id = all_projects[upstream_project]
                    if project_id not in project_list:
                        project_list.append(project_id)

    # If there are projects added to the project list, check whether these
    # projects have dependencies upstream by rerunning the
    # check_for_upstream_datasets recursively
    if project_list:
        return _find_upstream_projects(
            project=project,
            project_list=project_list,
            all_projects=all_projects,
            upstream_projects=upstream_projects,
        )
    else:
        # If the first project in the chain is reached, return the list of
        # projects that need to be updated

        # Reverse list of projects so that the first project in the chain is
        # returned first
        upstream_projects.reverse()

        upstream_projects = [
            project.client.projects.by_resource_id(project_id) for project_id in upstream_projects
        ]

        return upstream_projects


def get_upstream_projects(project: Project) -> List[Project]:
    """ Check for upstream projects associated with a specified project

    Args:
        project: the tamr project for which associated upstream projects are retrieved
    """

    # Retrieve upstream projects (if any) from a specified Project
    upstream_projects = _find_upstream_projects(project)

    return upstream_projects
