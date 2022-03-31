from typing import List, Optional, Union
import logging

from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.attribute.resource import AttributeSpec
from tamr_unify_client.attribute.type import AttributeType

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
    primary_keys: Optional[List[str]] = None,
    attributes: Optional[List[str]] = None,
    attribute_types: Optional[JsonDict] = None,
    description: Optional[str] = None,
    external_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
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
        attribute_types: dict of attribute types, attribute name is key and type is value
        description: text description of new dataset
        external_id: external_id for dataset, if None Tamr will create one for you
        tags: tags for new dataset

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: dataset or primary_keys must be defined
        ValueError: A dataset with name '{dataset_name}' already exists
    """

    if not dataset and not primary_keys:
        raise ValueError(f"dataset or primary_keys must be defined")

    # Get dataset information
    if dataset:
        # Get attributes from dataset object
        attribute_specs = [attr.spec() for attr in dataset.attributes.stream()]
        # Get dataset spec information
        description = dataset.description
        tags = dataset.tags
        primary_keys = dataset.key_attribute_names
    else:
        attribute_specs = _create_specs(
            attribute_names=attributes, attribute_types=attribute_types
        )

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

    # Get current dataset attributes
    target_dataset_attributes = target_dataset.attributes

    # Update attributes in dataset
    for attribute in attribute_specs:
        attr_spec_dict = attribute.to_dict()
        attribute_name = attr_spec_dict["name"]
        if attribute_name in primary_keys:
            # This attribute already exists
            continue
        else:
            # Create new attribute
            target_dataset_attributes.create(attr_spec_dict)
            LOGGER.info(f"Created attribute '{attribute_name}' in {dataset_name}")

    return target_dataset


def update(
    dataset: Dataset,
    *,
    attributes: Optional[List[str]] = None,
    attribute_types: Optional[JsonDict] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dataset:
    """Flexibly update a source dataset in Tamr. Will add/remove attributes to match input.
       If no attribute_types are passed in the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to add/keep for dataset
        attribute_types: dict of attribute types, attribute name is key and type is value
        description: updated text description of dataset, if None will not update
        tags: updated tags for dataset, if None will not update tags

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: {dataset_name} is not a source dataset
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")
    primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

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
                    dataset=dataset, attributes=[attribute_name], attribute_types=attribute_types
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
    *, dataset: Dataset, attributes: List[str], attribute_types: Optional[JsonDict] = None,
) -> Dataset:
    """Creates attributes in dataset if they don't already exist.
       If no attrbute_types are passed in, the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to add to dataset
        attribute_types: dict of attribute types, attribute name is key and type is value

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        TypeError: attributes arg must be a List
        ValueError: trying to alter a unified dataset
        ValueError: An attribute with name '{attribute_name}' already exists in {dataset_name}
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if type(attributes) != list:
        raise TypeError("attributes arg must be a List")

    # Create attributes from input
    attribute_specs = _create_specs(attribute_names=attributes, attribute_types=attribute_types)

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
    for attribute in attribute_specs:
        attr_spec_dict = attribute.to_dict()
        target_dataset_attributes.create(attr_spec_dict)
        attribute_name = attr_spec_dict["name"]
        LOGGER.info(f"Created attribute '{attribute_name}' in {dataset_name}")

    return dataset


def edit_attributes(
    *,
    dataset: Dataset,
    attributes: List[str],
    attribute_types: Optional[JsonDict] = None,
    attribute_descriptions: Optional[JsonDict] = None,
) -> Dataset:
    """Edits existing attributes in dataset.
       If an attrbute_type is not defined the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to edit in dataset
        attribute_types: dict of attribute types, attribute name is key and type is value
        attribute_descriptions: dict, attribute name is key and description is value

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: {dataset_name} is not a source dataset
        ValueError: An attribute with name '{attribute_name}' does not exist in {dataset_name}
        ValueError: The attribute: '{attribute_name}' is a primary key and can't be updated
        TypeError: attributes arg must be a List
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if type(attributes) != list:
        raise TypeError("attributes arg must be a List")

    # Create attributes from input
    attribute_specs = _create_specs(attribute_names=attributes, attribute_types=attribute_types)

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
    for attribute in attribute_specs:
        attr_spec_dict = attribute.to_dict()
        attribute_name = attr_spec_dict["name"]

        # Update type
        new_type = AttributeType(attribute.to_dict()["type"])
        target_attribute = target_attribute_dict[attribute_name]
        new_attr_spec = target_attribute.spec().with_type(new_type.spec())

        # Update description
        if attribute_descriptions and attribute_name in attribute_descriptions.keys():
            new_attr_spec = new_attr_spec.with_description(attribute_descriptions[attribute_name])

        # Remove and add attribute with new spec
        target_dataset_attributes.delete_by_resource_id(target_attribute.resource_id)
        target_dataset_attributes.create(new_attr_spec.to_dict())
        LOGGER.info(f"Updated attribute '{attribute_name}' in {dataset_name}")

    return dataset


def delete_attributes(*, dataset: Dataset, attributes: List[str] = None,) -> Dataset:
    """Remove attributes from dataset by attribute name

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to delete from dataset

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: {dataset_name} is not a source dataset
        ValueError: attribute with {attribute_name} does not exist in {dataset_name}
        ValueError: The attribute: '{attribute_name}' is a primary key and can't be removed
        TypeError: attributes arg must be a List
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check input type is correct
    if type(attributes) != list:
        raise TypeError("attributes arg must be a List")

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


def _create_specs(
    *, attribute_names: List[str], attribute_types: Union[JsonDict, None]
) -> List[AttributeSpec]:
    """Create list of attributeSpec. Use default type if none is given

    Args:
        attribute_names: List of names of attributes
        attribute_types: dict of attribute types, attribute name is key and type is value

    Return:
        List of AttributeSpecs
    """
    default_type = {
        "baseType": "ARRAY",
        "innerType": {"baseType": "STRING", "attributes": []},
        "attributes": [],
    }
    attribute_specs = []
    if attribute_names:
        for idx in range(len(attribute_names)):
            name = attribute_names[idx]
            if attribute_types and name in attribute_types.keys():
                attr_type = AttributeType(attribute_types[name])
            else:
                attr_type = AttributeType(default_type)
            attribute_specs.append(AttributeSpec.new().with_name(name).with_type(attr_type.spec()))
    return attribute_specs
