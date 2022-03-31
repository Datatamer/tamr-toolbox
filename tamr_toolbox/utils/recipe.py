"""Tasks related to project recipe"""
import logging

from tamr_unify_client.mastering.project import MasteringProject

from tamr_toolbox.models.project_type import ProjectType

LOGGER = logging.getLogger(__name__)


def get_dedup_recipe_id(project: MasteringProject) -> int:
    """
    Retrieves dedup recipe ID of a mastering project.
    To be used to run custom recipe operations.

    Args:
        project: a mastering project

    Returns: dedup recipe id of the project.

    """
    # Retrieve project steps using unified dataset
    project_steps = project.unified_dataset().usage().usage.input_to_project_steps
    # Search for dedup project step of the mastering project
    for step in project_steps:
        if step.project_name == project.name and step.type == ProjectType.DEDUP.value:
            recipe_id = step.project_step_id.split("/")[-1]
            return recipe_id
    # Return exception if cannot find dedup recipe of a mastering project
    not_found_error = (
        f"Cannot find dedup recipe for project {project.name}. "
        f"The project setup might not be complete."
    )
    LOGGER.error(not_found_error)
    raise Exception(not_found_error)
