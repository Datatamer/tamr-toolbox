"""Tasks related to metrics for Tamr Categorization projects"""
import logging

from tamr_unify_client.categorization.taxonomy import Taxonomy
from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.project.resource import Project

from tamr_toolbox.models import data_type
from tamr_toolbox.utils import operation

LOGGER = logging.getLogger(__name__)
CONFIDENCE_DATASET_SUFFIX = "classifications_average_confidences"


def _check_dataset_with_confidence(dataset: Dataset) -> bool:
    """
    Checks if the dataset contains necessary attributes for obtaining confidence information

    Args:
        dataset: Tamr dataset object

    Returns:
        True if the dataset contains required attributes to compute the average confidence;
        False otherwise

    Raises:
        RuntimeError: if the dataset does not contain 'classificationPath' or 'averageConfidence'
        attributes
    """
    # check required attributes are present
    attribute_names = [attribute.name for attribute in dataset.attributes.stream()]
    if "classificationPath" not in attribute_names or "averageConfidence" not in attribute_names:
        wrong_attribute_error = (
            f"You might be using a version of Tamr unsupported for this functionality because "
            f" 'classificationPath' and 'averageConfidence' attributes are missing are missing"
            f" from dataset {dataset.name}."
        )
        LOGGER.error(wrong_attribute_error)
        raise RuntimeError(wrong_attribute_error)
    return True


def _check_taxonomy_depth(project: Project, *, tier: int) -> bool:
    """
    Checks the maximum depth of the taxonomy associated wit

    Args:
        project: Tamr project object
        tier: integer specifying the tier from which to extract categories

    Returns:
        whether tier exceed the maximum taxonomy depth or not

    Raises:
        ValueError: if tier is greater than maximum taxonomy depth
    """

    # depth check is not required for leaf nodes
    if tier == -1:
        return True

    max_depth = 0
    classification_project = project.as_categorization()
    taxonomy = classification_project.taxonomy()
    categories = taxonomy.categories()

    for category in categories:
        if len(category.path) > max_depth:
            max_depth = len(category.path)

    if max_depth < tier:
        invalid_tier_value_error = (
            f"Invalid value for tier {tier}. Maximum depth detected is {max_depth}."
        )
        LOGGER.error(invalid_tier_value_error)
        raise ValueError(invalid_tier_value_error)
    return True


def _create_leaf_node_set(taxonomy: Taxonomy) -> set:
    """
    Extracts leaf nodes from taxonomy and returns them in a set.
    For a taxonomy with paths [[cat1], [cat1, cat11], [cat2], [cat1, cat12], [cat1, cat11, cat111]]
     the function returns set('cat2', 'cat1|cat11|cat111', 'cat1|cat12')

    Args:
        taxonomy: Tamr Taxonomy object for a categorization project

    Returns:
        Set of all leaf nodes in taxonomy, where node paths are joined by '|' if taxonomy is
        multi-level
   """
    all_set = set()
    parent_set = set()
    for category in taxonomy.categories().stream():
        all_set.add("|".join(category.path))
        if category.parent():
            parent_set.add("|".join(category.parent().path))
    leaf_set = all_set - parent_set
    return leaf_set


def _extract_confidence(*, dataset: Dataset, category_set: set) -> data_type.JsonDict:
    """
    Extracts tier-specific average confidence from a Tamr internal dataset
    `<unified dataset name>_classifications_average_confidences` to a dictionary

    Args:
        dataset: Tamr internal Dataset with a name ending in
        `_unified_dataset_classifications_average_confidences`
        category_set: set of category paths at the desired tier

    Returns:
        dictionary - keys are category paths, joined by '|' if multi-level taxonomy. Values are
        average confidence of the corresponding keys, where it is None if no confidence exists for
        the category.
    """
    confidence_dict = {}
    for record in dataset.records():
        path = "|".join(record["classificationPath"])
        if path in category_set:
            confidence_dict[path] = record["averageConfidence"]

    empty_confidence_categories = category_set - set(confidence_dict.keys())
    for category in empty_confidence_categories:
        confidence_dict[category] = None

    return confidence_dict


def _get_categories_at_tier(project: Project, *, tier: int) -> set:
    """
    Extracts categories at tier from a taxonomy associated with Project

    Args:
        project: Tamr project object
        tier: integer specifying the tier to extract the categories;
              -1 will return all leaf categories

    Returns:
        set of category paths at tier, joined by '|' if multi-level taxonomy
    """
    classification_project = project.as_categorization()
    taxonomy = classification_project.taxonomy()
    categories = taxonomy.categories()

    category_set = set()
    if tier > 0:
        for category in categories:
            if len(category.path) == tier:
                category_set.add("|".join(category.path))
    else:
        # leaf nodes
        category_set = _create_leaf_node_set(taxonomy)
    return category_set


def _get_dataset_with_confidence(project: Project) -> Dataset:
    """
    Retrieves internal dataset required to extract tier-specific average confidence

    Args:
        project: Tamr project object

    Returns:
        Dataset

    Raises:
        RuntimeError: if the required internal dataset does not exist in `project`
    """
    dataset_name = f"{project.unified_dataset().name}_{CONFIDENCE_DATASET_SUFFIX}"
    # check if the dataset can be retrieved
    try:
        dataset = project.client.datasets.by_name(dataset_name)
    except KeyError:
        dataset_not_found_error = (
            "Classification project could be incomplete, or you might be using a version of Tamr "
            "unsupported for this functionality because required dataset could not be found in "
            "the project datasets."
        )
        LOGGER.error(dataset_not_found_error)
        raise RuntimeError(dataset_not_found_error)
    return dataset


def get_tier_confidence(
    project: Project, *, tier: int = -1, allow_dataset_refresh: bool = False
) -> data_type.JsonDict:
    """
    Extracts tier-specific average confidence from a Tamr internal dataset
    `<unified dataset name>_classifications_average_confidences` in a dictionary

    Args:
        project: Tamr project object
        tier: integer specifying the tier to extract the average confidence;
              default value will return the average confidence at all leaf categories
        allow_dataset_refresh: if True, allows running a job to refresh dataset to make it
                               streamable

    Returns:
        dictionary - keys are category paths, joined by '|' if multi-level taxonomy and values are
        average confidence of the corresponding keys

    Raises:
        RuntimeError: if `dataset` is not streamable and `allow_dataset_refresh` is False;
        TypeError: if tier is not of type int;
                   or if the project type is not classification
        ValueError: if tier is less than -1 or equal to 0
    """
    LOGGER.info(
        f"Retrieving average confidence for taxonomy nodes in project {project.name} "
        f"(id={project.resource_id})."
    )
    # check project type is categorization
    try:
        project = project.as_categorization()
    except TypeError:
        not_categorization_error = f"Project {project.name} is not a classification project."
        LOGGER.error(not_categorization_error)
        raise TypeError(not_categorization_error)

    # check necessary dataset can be obtained
    dataset = _get_dataset_with_confidence(project)

    # check tier is valid
    if type(tier) is not int:
        wrong_tier_type_error = f"Tier {tier} is not an integer."
        LOGGER.error(wrong_tier_type_error)
        raise TypeError(wrong_tier_type_error)
    if tier < -1 or tier == 0:
        invalid_tier_value_error = (
            f"Invalid value for tier {tier}. Tier cannot be 0 or less than -1."
        )
        LOGGER.error(invalid_tier_value_error)
        raise ValueError(invalid_tier_value_error)

    # check dataset can be streamed
    if not dataset.status().is_streamable:
        if allow_dataset_refresh:
            LOGGER.info(f"Refreshing dataset {dataset.name} to make streamable.")
            op = dataset.refresh()
            operation.enforce_success(op)
        else:
            cannot_stream_error = (
                f"Dataset {dataset.name} is not streamable. "
                f"Refresh it first, or run with allow_dataset_refresh=True"
            )
            LOGGER.error(cannot_stream_error)
            raise RuntimeError(cannot_stream_error)

    # check dataset contains necessary attributes
    assert _check_dataset_with_confidence(dataset)

    # check tier does not exceed maximum taxonomy depth
    assert _check_taxonomy_depth(project, tier=tier)

    # obtain categories at tier
    selected_category_set = _get_categories_at_tier(project, tier=tier)

    # extract average confidence
    tier_confidence_dict = _extract_confidence(dataset=dataset, category_set=selected_category_set)
    return tier_confidence_dict
