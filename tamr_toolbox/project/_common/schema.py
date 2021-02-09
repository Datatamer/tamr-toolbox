"""Tasks related to schema mapping as part of Tamr projects"""
from tamr_unify_client.project.attribute_mapping.resource import (
    AttributeMappingSpec,
    AttributeMapping,
)
from tamr_unify_client.project.resource import Project
from tamr_unify_client.dataset.resource import Dataset
from typing import List
import logging
from json import JSONDecodeError

LOGGER = logging.getLogger(__name__)


def _get_mapping_spec_for_ud(
    *, source_ds_name: str, source_attr_name: str, unified_attr_name: str, unified_ds_name: str
) -> AttributeMappingSpec:
    """Makes an AttributeMappingSpec to be used to map attributes.
    Is a wrapper for its creation to hide internals in one place.

    Args:
        source_ds_name: the name of the source dataset
        source_attr_name: the name of the source attribute
        unified_attr_name: the name of the unified attribute
        unified_ds_name: the name of the unified dataset

    Returns:
        AttributeMappingSpec containing the passed information
    """
    spec = (
        AttributeMappingSpec.new()
        .with_input_attribute_name(source_attr_name)
        .with_input_dataset_name(source_ds_name)
        .with_unified_dataset_name(unified_ds_name)
        .with_unified_attribute_name(
            unified_attr_name
        )  # rest can be empty - Tamr will fill them in
        .with_input_attribute_id("")
        .with_relative_input_attribute_id("")
        .with_unified_attribute_id("")
        .with_relative_unified_attribute_id("")
    )
    return spec


def map_attribute(
    project: Project,
    *,
    source_attribute_name: str,
    source_dataset_name: str,
    unified_attribute_name: str,
) -> AttributeMapping:
    """
    Maps source_attribute in source_dataset to unified_attribute in unified_dataset.
    If the mapping already exists it will log
    a warning and return the existing AttributeMapping from the project's collection.

    Args:
        source_attribute_name: Source attribute name to map
        source_dataset_name: Source dataset containing the source attribute
        unified_attribute_name: Unified attribute to which to map the source attribute
        project: The project in which to perform the mapping

    Returns:
        AttributeMapping that was created

    Raises:
        ValueError: if input variables `source_attribute_name` or `source_dataset_name` or
            `unified_attribute_name` are set to empty strings;
            or if the dataset `source_dataset_name` is not found on Tamr;
            or if `source_attribute_name` is missing from the attributes of `source_attribute_name`
    """
    # simple validation, nothing should be empty
    variables = [source_attribute_name, source_dataset_name, unified_attribute_name]
    empty_variables = [x for x in variables if x == ""]
    if empty_variables:
        empty_variable_string = ", ".join(empty_variables)
        error_message = (
            f"The following variables are set to empty strings and "
            f"need to be filled in: {empty_variable_string} !"
        )
        LOGGER.error(error_message)
        raise ValueError(error_message)

    # also validate that the dataset exists and has this column
    try:
        source_dataset = project.client.datasets.by_name(source_dataset_name)
    except KeyError:
        error_msg = f"Dataset {source_dataset_name} not found!"
        LOGGER.error(error_msg)
        raise ValueError(error_msg)

    try:
        assert source_attribute_name in [x.name for x in source_dataset.attributes]
    except AssertionError:
        error_msg = f"Attribute {source_attribute_name} not found in {source_dataset_name}!"
        LOGGER.error(error_msg)
        raise ValueError(error_msg)

    # generate mapping spec
    mapping_spec = _get_mapping_spec_for_ud(
        source_attr_name=source_attribute_name,
        source_ds_name=source_dataset_name,
        unified_attr_name=unified_attribute_name,
        unified_ds_name=project.unified_dataset().name,
    )

    # add the mapping to the project's collection - this is what does the actual mapping
    try:
        return project.attribute_mappings().create(mapping_spec.to_dict())
    except JSONDecodeError as e:
        # can get a jsondecode error if the attribute is already mapped.
        # If it is, then log a warning and return empty mapping
        # if it is not already mapped break loudly
        m: AttributeMapping
        for m in project.attribute_mappings().stream():
            if (
                m.input_dataset_name == source_dataset_name
                and m.input_attribute_name == source_attribute_name
                and m.unified_attribute_name == unified_attribute_name
            ):
                # mapping exists, log warning and return existing mapping
                LOGGER.warning(
                    f"mapping of attribute {source_attribute_name} in dataset "
                    f"{source_dataset_name} to unified attribute {unified_attribute_name} "
                    f"already exists! Returning existing mapping spec"
                )
                return m

        # if haven't returned then throw the JSONDecodeError
        raise e


def unmap_attribute(
    project: Project,
    *,
    source_attribute_name: str,
    source_dataset_name: str,
    unified_attribute_name: str,
) -> None:
    """
    Unmaps a source attribute.

    Args:
        source_attribute_name: the name of the source attribute to unmap
        source_dataset_name: the name of the source dataset containing that source attribute
        unified_attribute_name: the unified attribute from which to unmap
        project: the project in which to unmap the attribute

    Returns:
        None
    """

    LOGGER.info(
        f"Trying to remove mapping of source attribute {source_attribute_name} in dataset "
        f"{source_dataset_name} from unified attribute {unified_attribute_name}"
    )

    # get mapping collection
    mapping_collection = project.attribute_mappings()

    # run through and get the resource id of the mapping to remove
    resource_id_to_remove = None
    for mapping in mapping_collection.stream():
        # consider it match if all of source attribute, source dataset and unified attribute
        # are equal
        if (
            source_attribute_name == mapping.input_attribute_name
            and source_dataset_name == mapping.input_dataset_name
            and unified_attribute_name == mapping.unified_attribute_name
        ):
            resource_id_to_remove = mapping.resource_id
            break

    # log warning if resource id wasn't found
    if resource_id_to_remove is None:
        LOGGER.warning(
            f"Mapping of {source_attribute_name} in dataset {source_dataset_name} to "
            f"unified attribute {unified_attribute_name} not found!"
        )
    # if found remove it
    else:
        mapping_collection.delete_by_resource_id(resource_id_to_remove)


def bootstrap_dataset(
    project: Project, *, source_dataset: Dataset, force_add_dataset_to_project: bool = False
) -> List[AttributeMapping]:
    """
    Bootstraps a dataset (i.e. maps all source columns to themselves)

    Args:
        source_dataset: the source dataset (a Dataset object not a string)
        project: the project to do the mapping ing
        force_add_dataset_to_project: boolean whether to add the dataset to the project
            if it is not already a part of it

    Returns:
        List of the AttributeMappings generated

    Raises:
        RuntimeError: if `source_dataset` is not part of the given `project`, set
            'force_add_dataset_to_project' flag to True to automatically add it
    """

    # check if dataset is in the project - python doesn't handle comparison of Dataset objects
    # well so check on name
    if source_dataset.name not in [x.name for x in project.input_datasets()]:
        if force_add_dataset_to_project:
            LOGGER.info(f"adding dataset {source_dataset.name} to project {project.name}")
            project.add_input_dataset(source_dataset)
        else:
            raise RuntimeError(
                f"dataset {source_dataset.name} not in project {project.name}!"
                + "Set 'force_add_dataset_to_project' flag to True to automatically add it"
            )

    # for each attribute map it
    source_dataset_name = source_dataset.name
    completed_mappings = []
    for attribute in source_dataset.attributes:
        attribute_name = attribute.name
        mapping = map_attribute(
            source_attribute_name=attribute_name,
            source_dataset_name=source_dataset_name,
            unified_attribute_name=attribute_name,
            project=project,
        )
        completed_mappings.append(mapping)

    return completed_mappings


def unmap_dataset(
    project: Project,
    *,
    source_dataset: Dataset,
    remove_dataset_from_project: bool = False,
    skip_if_missing: bool = False,
) -> None:
    """
    Wholly unmaps a dataset and optionally removes it from a project.

    Args:
        source_dataset: the source dataset (Dataset object not a string) to unmap
        project: the project in which to unmap the dataset
        remove_dataset_from_project: boolean to also remove the dataset from the project
        skip_if_missing: boolean to skip if dataset is not in project. If set to false and
            dataset is not in project will raise a RuntimeError

    Returns:
        None

    Raises:
        RuntimeError: if `source_dataset` is not in `project` and `skip_if_missing` not set to True
    """

    # check to make sure dataset is in project and log a warning if it is not
    if source_dataset.name not in [x.name for x in project.input_datasets()]:
        if skip_if_missing:
            LOGGER.warning(
                f"Dataset to unmap {source_dataset.name} not in project {project.name}! "
                f"However skip_if_missing flag is set so will do nothing"
            )
            return None
        else:
            error_message = (
                f"Dataset to unmap {source_dataset.name} not in project "
                f"{project.name} and skip_if_missing not set to True so failing! "
            )
            LOGGER.error(error_message)
            raise RuntimeError(error_message)

    # the resource ids of attribute mappings unfortunately change when you delete one
    # so need to just do this until there are no mappings left for the source dataset of interest
    while True:
        mappings = [
            x
            for x in project.attribute_mappings().stream()
            if x.input_dataset_name == source_dataset.name
        ]
        # if no mappings found for this dataset then break
        if not mappings:
            break
        for mapping in mappings:
            # can only delete one then have to break out of inner loop
            project.attribute_mappings().delete_by_resource_id(mapping.resource_id)
            break

    # optionally remove dataset from the project
    if remove_dataset_from_project:
        project.remove_input_dataset(source_dataset)
