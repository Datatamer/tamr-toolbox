from typing import Optional, Dict, Iterable
import logging

from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset
from tamr_toolbox.models import attribute_type

from tamr_toolbox.models.data_type import JsonDict

LOGGER = logging.getLogger(__name__)


def exists(*, client: Client, dataset_name: str) -> bool:
    """Check if the dataset exists on target instance

    Args:
        client: Tamr python client object for the target instance
        dataset_name: The dataset name

    Return:
        True or False for if the dataset exists in target instance
    """

    try:
        client.datasets.by_name(dataset_name)
    except KeyError:
        return False

    return True


def create(
    *,
    client: Client,
    dataset_name: str,
    dataset: Optional[Dataset] = None,
    primary_keys: Optional[Iterable[str]] = None,
    attributes: Optional[Iterable[str]] = None,
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    description: Optional[str] = None,
    external_id: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
) -> Dataset:
    """Flexibly create a source dataset in Tamr. Will use array string as default attribute type
       if none are specified. If a dataset object is passed, a new dataset with dataset_name as
       its name will be created that has the same attributes and primary keys as the dataset.

    Args:
        client: TUC client
        dataset_name: name for new dataset
        dataset: optional dataset TUC object to use as a template for new dataset
        primary_keys: one or more attributes for primary key(s) of new dataset
        attributes: list of attribute names for new dataset
        attribute_types: dict of attribute types, attribute name is key and AttributeType is value
        description: text description of new dataset
        external_id: external_id for dataset, if None Tamr will create one for you
        tags: tags for new dataset

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If both dataset and primary_keys are not defined
        ValueError: If the dataset already exists
        TypeError: If the attributes argument is not an Iterable
    """

    if not dataset and not primary_keys:
        raise ValueError(f"dataset or primary_keys must be defined")

    # Get dataset information
    if dataset:
        # Get attributes from dataset object
        attributes = []
        attribute_types = {}
        for attr in dataset.attributes.stream():
            attributes.append(attr.name)
            attribute_types[attr.name] = attribute_type.from_json(attr.type.spec().to_dict())
        # Get dataset spec information
        description = dataset.description
        tags = dataset.tags
        primary_keys = dataset.key_attribute_names

    # Check input type is correct
    if attributes and not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    dataset_exists = exists(client=client, dataset_name=dataset_name)
    if not dataset_exists:
        creation_spec = {
            "name": dataset_name,
            "description": description,
            "keyAttributeNames": primary_keys,
            "externalId": external_id,
            "tags": tags,
        }
        client.datasets.create(creation_spec)
        LOGGER.info(f"A dataset with name {dataset_name} has been created")
    else:
        raise ValueError(f"A dataset with name '{dataset_name}' already exists")

    # Get new dataset
    target_dataset = client.datasets.by_name(dataset_name)

    # Update attributes in dataset
    if attributes:
        filtered_attributes = [attr for attr in attributes if attr not in primary_keys]
        create_attributes(
            dataset=target_dataset, attributes=filtered_attributes, attribute_types=attribute_types
        )

    return target_dataset


def update(
    dataset: Dataset,
    *,
    attributes: Optional[Iterable[str]] = None,
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    description: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
    override_existing_types: bool = False,
) -> Dataset:
    """Flexibly update a source dataset in Tamr. Will add/remove attributes to match input.
       If no attribute_types are passed in the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to add/keep for dataset
        attribute_types: dict of attribute types, attribute name is key and AttributeType is value
        description: updated text description of dataset, if None will not update
        tags: updated tags for dataset, if None will not update tags
        override_existing_types: bool flag, when true will alter existing attributes

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If the dataset is not a source dataset
        TypeError: If the attributes argument is not an Iterable
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")
    primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

    # Check input type is correct
    if attributes and not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    # Update description and tags
    dataset_spec = dataset.spec()
    if description:
        dataset_spec = dataset_spec.with_description(description)
        LOGGER.info(f"Updating description for {dataset_name}")
    if tags:
        dataset_spec = dataset_spec.with_tags(tags)
        LOGGER.info(f"Updating tags for {dataset_name}")

    dataset_spec.put()

    if attributes:
        # Get current dataset attributes
        target_dataset_attributes = dataset.attributes
        existing_attributes = []
        for attr in target_dataset_attributes.stream():
            existing_attributes.append(attr.name)

        # Update attributes in dataset
        for attribute_name in attributes:
            if attribute_name in primary_keys:
                continue
            elif attribute_name in existing_attributes:
                # This attribute already exists, update to new type
                edit_attributes(
                    dataset=dataset,
                    attributes=[attribute_name],
                    attribute_types=attribute_types,
                    override_existing_types=override_existing_types,
                )
            else:
                # This attribute does not already exist, create
                create_attributes(
                    dataset=dataset, attributes=[attribute_name], attribute_types=attribute_types
                )

        # Remove any attributes from dataset that aren't in the new list of attributes
        for attribute_name in existing_attributes:
            if attribute_name not in attributes and attribute_name not in primary_keys:
                delete_attributes(dataset=dataset, attributes=[attribute_name])

    return dataset


def create_attributes(
    *,
    dataset: Dataset,
    attributes: Iterable[str],
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
) -> Dataset:
    """Creates attributes in dataset if they don't already exist.
       If no attribute_types are passed in, the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to add to dataset
        attribute_types: dict of attribute types, attribute name is key and AttributeType is value

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        TypeError: If the attributes argument is not an Iterable
        ValueError: If the dataset is a unified dataset
        ValueError: If an attribute passed in already exists in the dataset
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    # Get current dataset attributes
    target_dataset_attributes = dataset.attributes
    existing_attributes = []
    for attr in target_dataset_attributes.stream():
        existing_attributes.append(attr.name)

    # Check that none of the new attribute names already exist
    for attribute_name in attributes:
        if attribute_name in existing_attributes:
            # This attribute already exists
            raise ValueError(
                f"An attribute with name '{attribute_name}' already exists in {dataset_name}"
            )

    # Add attributes to dataset
    for attribute_name in attributes:
        attr_spec_dict = _make_spec_dict(
            attribute_name=attribute_name, attribute_types=attribute_types
        )
        target_dataset_attributes.create(attr_spec_dict)
        LOGGER.info(f"Created attribute '{attribute_name}' in {dataset_name}")

    return dataset


def edit_attributes(
    *,
    dataset: Dataset,
    attributes: Iterable[str],
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    attribute_descriptions: Optional[JsonDict] = None,
    override_existing_types: bool = False,
) -> Dataset:
    """Edits existing attributes in dataset.
       If an attribute_type is not defined the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to edit in dataset
        attribute_types: dict of attribute types, attribute name is key and AttributeType is value
        attribute_descriptions: dict, attribute name is key and description is value
        override_existing_types: bool flag, when true will alter exisiting attributes

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If the dataset is not a source dataset
        ValueError: If a passed attribute does not exist in the dataset
        ValueError: If a passed attribute is a primary key and can't be removed
        TypeError: If the attributes argument is not an Iterable
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    # Get current dataset attributes
    target_dataset_attributes = dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr
    existing_attributes = target_attribute_dict.keys()
    primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

    # Check that all of the attribute names already exist in dataset
    for attribute_name in attributes:
        if attribute_name not in existing_attributes:
            # This attribute does not exist
            raise ValueError(
                f"An attribute with name '{attribute_name}' does not exist in {dataset_name}"
            )
        elif attribute_name in primary_keys:
            # Can not edit a primary key
            raise ValueError(
                f"The attribute: '{attribute_name}' is a primary key and can't be updated"
            )

    # Update attributes in dataset
    for attribute_name in attributes:
        attr_spec_dict = _make_spec_dict(
            attribute_name=attribute_name, attribute_types=attribute_types
        )
        existing_attribute_spec = target_attribute_dict[attribute_name].spec()
        new_type_class = attribute_type.from_json(attr_spec_dict["type"])
        old_type_class = attribute_type.from_json(existing_attribute_spec.to_dict()["type"])

        if new_type_class == old_type_class:
            # Update description
            if attribute_descriptions and attribute_name in attribute_descriptions.keys():
                existing_attribute_spec = existing_attribute_spec.with_description(
                    attribute_descriptions[attribute_name]
                )
                existing_attribute_spec.put()
        elif override_existing_types:
            # Update type
            new_attr_spec = existing_attribute_spec.to_dict()
            new_attr_spec["type"] = attr_spec_dict["type"]

            # Update description
            if attribute_descriptions and attribute_name in attribute_descriptions.keys():
                new_attr_spec["description"] = attribute_descriptions[attribute_name]

            # Remove and add attribute with new spec
            target_dataset_attributes.delete_by_resource_id(
                target_attribute_dict[attribute_name].resource_id
            )
            target_dataset_attributes.create(new_attr_spec)
            LOGGER.info(f"Updated attribute '{attribute_name}' in {dataset_name}")
        else:
            LOGGER.info(
                f"""The attribute '{attribute_name}' in {dataset_name} curently has
                 the type '{str(old_type_class)}'. Set '{override_existing_types}' to
                 True to update the type to '{str(new_type_class)}
                """
            )

    return dataset


def delete_attributes(*, dataset: Dataset, attributes: Iterable[str] = None,) -> Dataset:
    """Remove attributes from dataset by attribute name

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to delete from dataset

    Returns:
        Updated Dataset

    Raises:
        ValueError: If the dataset is not a source dataset
        ValueError: If a passed attribute does not exist in the dataset
        ValueError: If a passed attribute is a primary key and can't be removed
        TypeError: If the attributes argument is not an Iterable
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    # Get current dataset attributes
    target_dataset_attributes = dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr
    existing_attributes = target_attribute_dict.keys()
    primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

    # Check all attributes exist before starting to remove any
    for attribute_name in attributes:
        if attribute_name not in existing_attributes:
            raise ValueError(
                f"attribute with name '{attribute_name}' does not exist in {dataset_name}"
            )
        elif attribute_name in primary_keys:
            # Can not edit a primary key
            raise ValueError(
                f"The attribute: '{attribute_name}' is a primary key and can't be removed"
            )

    # Remove attributes from dataset
    for attribute_name in attributes:
        target_dataset_attributes.delete_by_resource_id(
            target_attribute_dict[attribute_name].resource_id
        )
        LOGGER.info(f"Deleted attribute '{attribute_name}' in {dataset_name}")

    return dataset


def _make_spec_dict(
    attribute_name: str, attribute_types: Dict[str, attribute_type.AttributeType]
) -> JsonDict:
    """Create attribute spec dictionary

    Args:
        attribute_name: name of the attribute
        attribute_types: dict of attribute types, attribute name is key and AttributeType is value

    Returns:
        Json Dict
    """
    if attribute_types and attribute_name in attribute_types.keys():
        attr_type = attr_type = attribute_types[attribute_name]
    else:
        attr_type = attribute_type.DEFAULT
    result = {"name": attribute_name, "type": attribute_type.to_json(attr_type=attr_type)}
    return result
