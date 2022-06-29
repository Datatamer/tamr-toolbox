from typing import List, Optional, Dict, Iterable
import logging

from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset
from tamr_toolbox.models import attribute_type

from tamr_toolbox.models.data_type import JsonDict

LOGGER = logging.getLogger(__name__)


def exists(*, client: Client, dataset_name: str) -> bool:
    """
    Check if a dataset exists in a Tamr instance

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
    attributes: Optional[Iterable[str]] = None,
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    attribute_descriptions: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    external_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dataset:
    """Flexibly create a source dataset in Tamr

    A template dataset object can be passed in to create a duplicate dataset with a new name. If
    the template dataset is not provided, the primary_keys must be defined for the dataset to be
    created. Additional attributes can be added in the attributes argument. The default attribute
    type will be ARRAY STRING. Non-default attribute types can be specified in the attribute_types
    dictionary. Any attribute descriptions can be specified in the attribute_descriptions
    dictionary.

    Args:
        client: TUC client object
        dataset_name: name for the new dataset being created
        dataset: optional dataset TUC object to use as a template for the new dataset
        primary_keys: one or more attributes for primary key(s) of the new dataset
        attributes: a list of attribute names to create in the new dataset
        attribute_types: dictionary for non-default types, attribute name is the key and
            AttributeType is the value
        attribute_descriptions: dictionary for attribute descriptions, attribute name is the key
            and the attribute description is the value
        description: description of the new dataset
        external_id: external_id for dataset, if None Tamr will create one for you
        tags: the list of tags for the new dataset

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If both dataset and primary_keys are not defined
        ValueError: If the dataset already exists
        TypeError: If the attributes argument is not an Iterable

    Example:
        >>> import tamr_toolbox as tbox
        >>> tamr_client = tbox.utils.client.create(**instance_connection_info)
        >>> tbox.dataset.manage.create(
        >>>     client=tamr_client,
        >>>     dataset_name="my_new_dataset",
        >>>     primary_keys=["unique_id"],
        >>>     attributes=["name","address"],
        >>>     description="My new dataset",
        >>> )
    """

    if not dataset and not primary_keys:
        raise ValueError(f"dataset or primary_keys must be defined")

    # Get dataset information
    if dataset:
        # Get attributes from dataset object
        attribute_types, attribute_descriptions = {}, {}
        for attr in dataset.attributes.stream():
            attribute_types[attr.name] = attribute_type.from_json(attr.type.spec().to_dict())
            attribute_descriptions[attr.name] = attr.description
        attributes = attribute_types.keys()
        # Get dataset spec information
        description = dataset.description
        tags = dataset.tags
        primary_keys = dataset.key_attribute_names

    # Check input type is correct
    if attributes and not isinstance(attributes, Iterable):
        raise TypeError("attributes arg must be an Iterable")

    if not exists(client=client, dataset_name=dataset_name):
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
            dataset=target_dataset,
            attributes=filtered_attributes,
            attribute_types=attribute_types,
            attribute_descriptions=attribute_descriptions,
        )

    return target_dataset


def update(
    dataset: Dataset,
    *,
    attributes: Optional[Iterable[str]] = None,
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    attribute_descriptions: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    override_existing_types: bool = False,
) -> Dataset:
    """Flexibly update a source dataset in Tamr

    All the attributes that should exist in the dataset must be defined in the attributes argument.
    This function will add/remove attributes in the dataset until the dataset attributes matches
    the set of attributes passed in as an argument. The default attribute type will be ARRAY
    STRING . To set non-default attribute types, they must be defined in the attribute_types
    dictionary. Any attribute descriptions can be specified in the attribute_descriptions
    dictionary. By default, the existing attribute types will not change unless
    override_existing_types is set to True. When False, the attribute type updates will only be
    logged.

    Args:
        dataset: An existing TUC dataset
        attributes: Complete list of attribute names that should exist in the updated dataset
        attribute_types: dictionary for non-default types, attribute name is the key and
            AttributeType is the value
        attribute_descriptions: dictionary for attribute descriptions, attribute name is the
            key and the attribute description is the value
        description: updated description of dataset, if None will not update the description
        tags: updated tags for the dataset, if None will not update tags
        override_existing_types: boolean flag, when true will alter existing attribute's types

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If the dataset is not a source dataset
        TypeError: If the attributes argument is not an Iterable

    Example:
        >>> import tamr_toolbox as tbox
        >>> from tbox.models import attribute_type
        >>> tamr_client = tbox.utils.client.create(**instance_connection_info)
        >>> dataset = = tamr_client.datasets.by_name("my_dataset_name")
        >>> tbox.dataset.manage.update(
        >>>     client=tamr_client,
        >>>     dataset=dataset,
        >>>     attributes=["unique_id","name","address","total_sales"],
        >>>     attribute_types={"total_sales":attribute_type.ARRAY(attribute_type.DOUBLE)},
        >>>     override_existing_types = True,
        >>> )
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
        existing_attributes = [attr.name for attr in dataset.attributes]

        # Update attributes in dataset
        for attribute_name in attributes:
            if attribute_name in primary_keys:
                continue
            elif attribute_name in existing_attributes:
                # This attribute already exists, update to new type
                type_dict = {
                    attribute_name: (attribute_types or dict()).get(
                        attribute_name, attribute_type.DEFAULT
                    )
                }
                desc_dict = {
                    attribute_name: (attribute_descriptions or dict()).get(attribute_name)
                }

                edit_attributes(
                    dataset=dataset,
                    attribute_types=type_dict,
                    attribute_descriptions=desc_dict,
                    override_existing_types=override_existing_types,
                )
            else:
                # This attribute does not already exist, create
                create_attributes(
                    dataset=dataset,
                    attributes=[attribute_name],
                    attribute_types=attribute_types,
                    attribute_descriptions=attribute_descriptions,
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
    attribute_descriptions: Optional[Dict[str, str]] = None,
) -> Dataset:
    """Create new attributes in a dataset

    The default attribute type will be ARRAY STRING. To set non-default attribute types, they must
    be defined in the attribute_types dictionary. Any attribute descriptions can be specified in
    the attribute_descriptions dictionary.

    Args:
        dataset: An existing TUC dataset
        attributes: list of attribute names to be added to dataset
        attribute_types: dictionary for non-default types, attribute name is the key and
            AttributeType is the value
        attribute_descriptions: dictionary for attribute descriptions, attribute name is the
            key and the attribute description is the value

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
    existing_attributes = [attr.name for attr in dataset.attributes]

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
            attribute_name=attribute_name,
            attribute_types=attribute_types,
            attribute_descriptions=attribute_descriptions,
        )
        dataset.attributes.create(attr_spec_dict)
        LOGGER.info(f"Created attribute '{attribute_name}' in {dataset_name}")

    return dataset


def edit_attributes(
    *,
    dataset: Dataset,
    attribute_types: Optional[Dict[str, attribute_type.AttributeType]] = None,
    attribute_descriptions: Optional[Dict[str, str]] = None,
    override_existing_types: bool = True,
) -> Dataset:
    """Edit existing attributes in a dataset

    The attribute type and/or descriptions can be updated to new values. Attributes that will be
    updated must be in either the attribute_types or attribute_descriptions dictionaries or
    both. The default attribute type will be ARRAY STRING. To set non-default attribute types, they
    must be defined in the attribute_types dictionary. Any attribute descriptions can be specified
    in the attribute_descriptions dictionary. If only the attribute_descriptions dictionary is
    defined, the attribute type will not be updated.

    Args:
        dataset: An existing TUC dataset
        attribute_types: dictionary for non-default types, attribute name is the key and
            AttributeType is the value
        attribute_descriptions: dictionary for attribute descriptions, attribute name is the
            key and the attribute description is the value
        override_existing_types: bool flag, when true will alter existing attributes

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: If the dataset is not a source dataset
        ValueError: If a passed attribute does not exist in the dataset
        ValueError: If a passed attribute is a primary key and can't be removed
        ValueError: If there are no updates to attributes in attribute_types or
            attribute_descriptions arguments
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Check description or type changes are passed in
    if attribute_types is None and attribute_descriptions is None:
        raise ValueError(
            """Updates to attributes must be passed in via attribute_types
            or attribute_descriptions arguments"""
        )

    # Get list of attributes that need updating from attribute_types and
    # attribute_descriptions dictionaries
    attributes = {attr for attr in attribute_types or list()} | {
        attr for attr in attribute_descriptions or list()
    }

    # Get current dataset attributes
    target_attribute_dict = {attr.name: attr for attr in dataset.attributes}
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
                f"The attribute '{attribute_name}' is a primary key and can't be updated"
            )

    # Update attributes in dataset
    for attribute_name in attributes:
        attr_spec_dict = _make_spec_dict(
            attribute_name=attribute_name,
            attribute_types=attribute_types,
            attribute_descriptions=attribute_descriptions,
        )
        existing_attribute_spec = target_attribute_dict[attribute_name].spec()
        if attribute_types is None or attribute_name not in attribute_types:
            new_type_class = attribute_type.from_json(existing_attribute_spec.to_dict()["type"])
        else:
            new_type_class = attribute_type.from_json(attr_spec_dict["type"])
        old_type_class = attribute_type.from_json(existing_attribute_spec.to_dict()["type"])

        if new_type_class == old_type_class:
            # Update description
            if (
                attribute_descriptions is not None
                and attribute_name in attribute_descriptions.keys()
            ):
                existing_attribute_spec = existing_attribute_spec.with_description(
                    attribute_descriptions[attribute_name]
                )
                existing_attribute_spec.put()
            else:
                LOGGER.info(
                    f"There are no updates to the attribute '{attribute_name}' in {dataset_name}"
                )
        elif override_existing_types:
            # Update type
            new_attr_spec = existing_attribute_spec.to_dict()
            new_attr_spec["type"] = attr_spec_dict["type"]

            # Update description
            if "description" in attr_spec_dict.keys():
                new_attr_spec["description"] = attr_spec_dict["description"]

            # Remove and add attribute with new spec
            dataset.attributes.delete_by_resource_id(
                target_attribute_dict[attribute_name].resource_id
            )
            dataset.attributes.create(new_attr_spec)
            LOGGER.info(f"Updated attribute '{attribute_name}' in {dataset_name}")
        else:
            LOGGER.info(
                f"""The attribute '{attribute_name}' in {dataset_name} curently has
                 the type '{str(old_type_class)}'. Set 'override_existing_types' to
                 True to update the type to '{str(new_type_class)}'
                """
            )

    return dataset


def delete_attributes(*, dataset: Dataset, attributes: Iterable[str] = None) -> Dataset:
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
    target_attribute_dict = {attr.name: attr for attr in dataset.attributes}
    existing_attributes = target_attribute_dict.keys()
    primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

    # Check all attributes exist before starting to remove any
    for attribute_name in attributes:
        if attribute_name not in existing_attributes:
            raise ValueError(f"The attribute '{attribute_name}' does not exist in {dataset_name}")
        elif attribute_name in primary_keys:
            # Can not edit a primary key
            raise ValueError(
                f"The attribute '{attribute_name}' is a primary key and can't be removed"
            )

    # Remove attributes from dataset
    for attribute_name in attributes:
        dataset.attributes.delete_by_resource_id(target_attribute_dict[attribute_name].resource_id)
        LOGGER.info(f"Deleted attribute '{attribute_name}' in {dataset_name}")

    return dataset


def _make_spec_dict(
    attribute_name: str,
    attribute_types: Dict[str, attribute_type.AttributeType],
    attribute_descriptions: Dict[str, str],
) -> JsonDict:
    """Create attribute spec dictionary

    The default attribute type will be ARRAY STRING. Non-default attribute types can be specified
    in the attribute_types dictionary. Any attribute descriptions can be specified in the
    attribute_descriptions dictionary.

    Args:
        attribute_name: name of the attribute
        attribute_types: dictionary for non-default types, attribute name is the key and
            AttributeType is the value
        attribute_descriptions: dictionary for attribute descriptions, attribute name is the
            key and the attribute description is the value

    Returns:
        Json Dict
    """
    if attribute_types is not None and attribute_name in attribute_types.keys():
        attr_type = attribute_types[attribute_name]
    else:
        attr_type = attribute_type.DEFAULT

    result = {"name": attribute_name, "type": attribute_type.to_json(attr_type=attr_type)}

    if attribute_descriptions is not None and attribute_name in attribute_descriptions.keys():
        result["description"] = attribute_descriptions[attribute_name]
    return result
