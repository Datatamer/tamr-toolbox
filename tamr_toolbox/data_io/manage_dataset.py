from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset, DatasetSpec
from typing import List, Optional, Union
from tamr_unify_client.attribute.resource import AttributeSpec
from tamr_unify_client.attribute.type import AttributeType


def create_dataset(
    tamr: Client, dataset_spec: DatasetSpec, attributes: Union[List[str], List[AttributeSpec]],
) -> Dataset:
    """Flexibly create a source dataset in Tamr. Will use array string as deafult attribute type.

    Args:
        tamr: TUC client
        dataset_spec: A representation of the server view of a dataset
        attributes: List of all attributes

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: primary key is not in attributes
        ValueError: A dataset with name '{dataset_name}' already exists
    """

    creation_spec_dict = dataset_spec.to_dict()
    dataset_name = creation_spec_dict["name"]
    primary_keys = creation_spec_dict["keyAttributeNames"]

    dataset_exists = _check_dataset_exists(target_instance=tamr, dataset=dataset_name)
    if not dataset_exists:
        tamr.datasets.create(creation_spec_dict)
    else:
        raise ValueError(f"A dataset with name '{dataset_name}' already exists")

    # Get new dataset
    target_dataset = tamr.datasets.by_name(dataset_name)

    # Get current dataset attributes
    target_dataset_attributes = target_dataset.attributes

    # Attributes to add to dataset
    if type(attributes[0]) is not AttributeSpec:
        attributes = _create_default_specs(attributes)

    # Update attributes in dataset
    for attribute in attributes:
        attr_spec_dict = attribute.to_dict()
        attribute_name = attr_spec_dict["name"]
        if attribute_name in primary_keys:
            # This attribute already exists
            continue
        else:
            # Create new attribute
            target_dataset_attributes.create(attr_spec_dict)

    return target_dataset


def modify_dataset(
    dataset: Dataset,
    new_dataset_spec: Optional[DatasetSpec] = None,
    attributes: Optional[Union[List[str], List[AttributeSpec]]] = None,
) -> Dataset:
    """Flexibly update a source dataset in Tamr. Will add/remove attributes to match input

    Args:
        dataset: An existing TUC dataset
        new_dataset_spec: Spec of the new dataset
        attributes: List of all attributes

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: trying to alter a unified dataset
    """
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset.name} is not a source dataset")

    if new_dataset_spec:
        # Update description and tags
        dataset_spec = dataset.spec()

        new_spec_dict = new_dataset_spec.to_dict()
        new_description = new_spec_dict["description"]
        new_tags = new_spec_dict["tags"]

        dataset_spec = dataset_spec.with_description(new_description).with_tags(new_tags)
        dataset_spec.put()

    if attributes:
        # Attributes to add to dataset
        if type(attributes[0]) is not AttributeSpec:
            attributes = _create_default_specs(attributes)

        # Get current dataset attributes
        target_dataset_attributes = dataset.attributes
        target_attribute_dict = {}
        for attr in target_dataset_attributes.stream():
            target_attribute_dict[attr.name] = attr
        primary_keys = dataset.spec().to_dict()["keyAttributeNames"]

        # Update attributes in dataset
        for attribute in attributes:
            attr_spec_dict = attribute.to_dict()
            attribute_name = attr_spec_dict["name"]
            if attribute_name in primary_keys:
                continue
            elif attribute_name in target_attribute_dict.keys():
                # This attribute already exists, update to new type
                new_type = AttributeType(attribute.to_dict()["type"])
                target_attribute = target_attribute_dict[attribute_name]
                new_attr_spec = target_attribute.spec().with_type(new_type.spec())

                # Remove and add attribute with new type
                target_dataset_attributes.delete_by_resource_id(target_attribute.resource_id)
                target_dataset_attributes.create(new_attr_spec.to_dict())
            else:
                # This attribute does not already exist, create
                target_dataset_attributes.create(attr_spec_dict)

        # Remove any attributes from dataset that aren't in the new list of attributes
        attribute_names = [attr.to_dict()["name"] for attr in attributes]
        for existing_attribute in target_attribute_dict.keys():
            if existing_attribute not in attribute_names:
                target_dataset_attributes.delete_by_resource_id(
                    target_attribute_dict[existing_attribute].resource_id
                )

    return dataset


def _create_default_specs(attribute_names: List[str]) -> List[AttributeSpec]:
    """Create list of attributeSpec with default type

    Args:
        attribute_one: first type
        attribute_two: second type

    Return:
        True or False for if the types match
    """
    default_type = AttributeType(
        {
            "baseType": "ARRAY",
            "innerType": {"baseType": "STRING", "attributes": []},
            "attributes": [],
        }
    )
    attribute_specs = []
    for name in attribute_names:
        attribute_specs.append(AttributeSpec.new().with_name(name).with_type(default_type.spec()))
    return attribute_specs


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
