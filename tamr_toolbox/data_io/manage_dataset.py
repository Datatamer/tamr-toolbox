from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset
from typing import List, Optional, Union
from tamr_unify_client.attribute.resource import Attribute
from tamr_unify_client.attribute.type import AttributeType

DEFAULT_TYPE = AttributeType({"baseType": "ARRAY", "innerType": {"baseType": "STRING"}})


def create_dataset(
    tamr: Client,
    dataset_name: str,
    attributes: Union[List[str], List[Attribute]],
    primary_keys: Union[str, List[str]],
    description: Optional[str] = None,
    external_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dataset:
    """Flexibly create a source dataset in Tamr.

    Args:
        tamr: Tamr instance
        dataset_name: Dataset name
        attributes: List of all attribute names
        primary_keys: Dataset primary key attribute names
        description: Dataset description
        external_id: External ID of the dataset
        tags: tags for dataset

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: trying to alter a unified dataset
        ValueError: attributes must be defined
        ValueError: primary key is not in attributes
        ValueError: A dataset with name '{dataset_name}' already exists
    """

    if not attributes:
        raise ValueError(f"Attributes must be defined")

    if type(attributes[0]) is Attribute:
        use_default_type = False
        attribute_names = [a.name for a in attributes]
    else:
        use_default_type = True
        attribute_names = attributes

    if type(primary_keys) is str:
        primary_keys = [primary_keys]

    for primary_key in primary_keys:
        if primary_key not in attribute_names:
            raise ValueError(f"The primary key '{primary_key}' is not in attributes")

    dataset_exists = _check_dataset_exists(target_instance=tamr, dataset=dataset_name)
    if not dataset_exists:
        creation_spec = {
            "name": dataset_name,
            "description": description,
            "keyAttributeNames": primary_keys,
            "externalId": external_id,
            "tags": tags,
        }
        tamr.datasets.create(creation_spec)
    else:
        raise ValueError(f"A dataset with name '{dataset_name}' already exists")

    # Get new dataset
    target_dataset = tamr.datasets.by_name(dataset_name)

    # Get current dataset attributes
    target_dataset_attributes = target_dataset.attributes

    # Update attributes in dataset
    for idx in range(len(attributes)):
        attribute_name = attribute_names[idx]
        if attribute_name in primary_keys:
            # This attribute already exists
            continue

        # Create new attribute
        if use_default_type:
            attr_spec = {
                "name": attribute_name,
                "type": DEFAULT_TYPE.spec().to_dict(),
            }
        else:
            attr_spec = attributes[idx].spec().to_dict()

        target_dataset_attributes.create(attr_spec)

    return target_dataset


def modify_dataset(
    tamr: Client,
    dataset_name: str,
    attributes: Union[List[str], List[Attribute]],
    primary_keys: Union[str, List[str]],
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dataset:
    """Flexibly update a source dataset in Tamr. Will add/remove attributes to match input

    Args:
        tamr: Tamr instance
        dataset_name: Dataset name
        attributes: List of all attribute names
        primary_keys: Dataset primary key attribute names
        description: Dataset description
        tags: tags for dataset

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: trying to alter a unified dataset
        ValueError: attributes must be defined
        ValueError: primary key is not in attributes
        ValueError: A dataset with name '{dataset_name}' does not exist
    """

    if not attributes:
        raise ValueError(f"Attributes must be defined")

    if type(attributes[0]) is Attribute:
        use_default_type = False
        attribute_names = [a.name for a in attributes]
    else:
        use_default_type = True
        attribute_names = attributes

    if type(primary_keys) is str:
        primary_keys = [primary_keys]

    for primary_key in primary_keys:
        if primary_key not in attribute_names:
            raise ValueError(f"The primary key '{primary_key}' is not in attributes")

    dataset_exists = _check_dataset_exists(target_instance=tamr, dataset=dataset_name)
    if not dataset_exists:
        raise ValueError(f"A dataset with name '{dataset_name}' does not exist")

    target_dataset = tamr.datasets.by_name(dataset_name)
    if target_dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Update description and/or tags
    new_dataset_spec = target_dataset.spec()

    if description and description != target_dataset.description:
        new_dataset_spec = new_dataset_spec.with_description(description)

    if tags and tags != target_dataset.tags:
        new_dataset_spec = new_dataset_spec.with_tags(tags)
    new_dataset_spec.put()

    # Get current dataset attributes
    target_dataset_attributes = target_dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr

    # Update attributes in dataset
    for idx in range(len(attributes)):
        attribute_name = attribute_names[idx]
        if attribute_name in target_attribute_dict.keys():
            # This attribute already exists, update type if needed
            if (
                not use_default_type
                and attribute_name not in primary_keys
                and not _same_attribute_type_check(
                    attributes[idx], target_attribute_dict[attribute_name]
                )
            ):
                new_type = attributes[idx].type.spec()
                new_attr_spec = target_attribute_dict[attribute_name].spec().with_type(new_type)
                # remove and add atrtibute with new type
                target_dataset_attributes.delete_by_resource_id(
                    target_attribute_dict[attribute_name].resource_id
                )
                target_dataset_attributes.create(new_attr_spec.to_dict())
            continue

        # Create new attribute
        if use_default_type:
            attr_spec = {
                "name": attribute_name,
                "type": DEFAULT_TYPE.spec().to_dict(),
            }
        else:
            attr_spec = attributes[idx].spec().to_dict()

        target_dataset_attributes.create(attr_spec)

    # Remove any attributes from dataset that aren't in the new list of attributes
    for existing_attribute in target_attribute_dict.keys():
        if existing_attribute not in attribute_names:
            target_dataset_attributes.delete_by_resource_id(
                target_attribute_dict[existing_attribute].resource_id
            )

    return target_dataset


def _same_attribute_type_check(attribute_one: Attribute, attribute_two: Attribute) -> bool:
    """Check if the two Attribute Types match

    Args:
        attribute_one: first type
        attribute_two: second type

    Return:
        True or False for if the types match
    """
    # To Do: Add aditional logic here for complex types and subattributes

    type_dict_one = attribute_one.type.spec().to_dict()
    type_dict_two = attribute_two.type.spec().to_dict()

    if type_dict_one == type_dict_two:
        return True

    return False


def _check_dataset_exists(*, target_instance: Client, dataset: str) -> bool:
    """Check if the dataset exists on target instance

    Args:
        target_instance: Tamr python client object for the target instance
        dataset: The dataset name

    Return:
        True or False for if the dataset exists in target instance
    """

    try:
        target_instance.datasets.by_name(dataset)
    except KeyError:
        return False

    return True
