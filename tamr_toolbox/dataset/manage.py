from typing import List, Optional, Union

from tamr_unify_client import Client
from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.attribute.resource import AttributeSpec
from tamr_unify_client.attribute.type import AttributeType

from tamr_toolbox.models.data_type import JsonDict


def create(
    *,
    client: Client,
    dataset_name: str,
    primary_keys: Optional[List[str]] = None,
    attributes: Optional[List[str]] = None,
    attribute_types: Optional[JsonDict] = None,
    description: Optional[str] = None,
    external_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    dataset: Optional[Dataset] = None,
) -> Dataset:
    """Flexibly create a source dataset in Tamr. Will use array string as default attribute type
       if none are specified. If a dataset object is passed, a new dataset with dataset_name as
       its name will be created that has the same attributes and primary keys as the dataset.

    Args:
        tamr: TUC client
        dataset_name: name for new dataset
        primary_keys: one or more attributes for primary key(s) of new dataset
        attributes: list of attribute names for new dataset
        attribute_types: dict of attribute types, attribute name is key and type is value
        description: text description of new dataset
        external_id: external_id for dataset, if None Tamr will create one for you
        tags: tags for new dataset
        dataset: optional dataset TUC object that the new dataset will replicate

    Returns:
        Dataset created in Tamr

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: Dataset or primary_keys must be defined
        ValueError: A dataset with name '{dataset_name}' already exists
    """

    if not dataset and not primary_keys:
        raise ValueError(f"Dataset or primary_keys must be defined")

    # Get dataset information
    if dataset:
        # Get attributes from dataset object
        attribute_specs = [attr.spec() for attr in dataset.attributes.stream()]
        # Get dataset spec information
        description = dataset.description
        tags = dataset.tags
        primary_keys = dataset.key_attribute_names
    else:
        # Create attributes from input
        attribute_specs = _create_specs(
            attribute_names=attributes, attribute_types=attribute_types
        )

    dataset_exists = exists(client=client, dataset=dataset_name)
    if not dataset_exists:
        creation_spec = {
            "name": dataset_name,
            "description": description,
            "keyAttributeNames": primary_keys,
            "externalId": external_id,
            "tags": tags,
        }
        client.datasets.create(creation_spec)
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

    return target_dataset


def update(
    *,
    dataset: Dataset,
    primary_keys: List[str],
    attributes: Optional[List[str]] = None,
    attribute_types: Optional[JsonDict] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Dataset:
    """Flexibly update a source dataset in Tamr. Will add/remove attributes to match input.
       If no attrbute_types are passed in the default will be ARRAY STRING

    Args:
        dataset: An existing TUC dataset
        primary_keys: one or more attributes for primary key(s) of new dataset
        attributes: list of attribute names to add/keep for dataset
        attribute_types: dict of attribute types, attribute name is key and type is value
        description: updated text description of dataset, if None will not update
        tags: updated tags for dataset, if None will not update tags

    Returns:
        Updated Dataset

    Raises:
        requests.HTTPError: If any HTTP error is encountered
        ValueError: trying to alter a unified dataset
        ValueError: Primary keys did not match Tamr dataset primary keys
    """
    dataset_name = dataset.name
    if dataset.upstream_datasets():
        raise ValueError(f"{dataset_name} is not a source dataset")

    # Confirm primary keys match
    if primary_keys != dataset.spec().to_dict()["keyAttributeNames"]:
        raise ValueError(f"Primary keys did not match Tamr dataset primary keys")

    # Update description and tags
    dataset_spec = dataset.spec()
    if description:
        dataset_spec = dataset_spec.with_description(description)
    if tags:
        dataset_spec = dataset_spec.with_tags(tags)

    dataset_spec.put()

    if attributes:
        # Create attributes from input
        attribute_specs = _create_specs(
            attribute_names=attributes, attribute_types=attribute_types
        )

        # Get current dataset attributes
        target_dataset_attributes = dataset.attributes
        target_attribute_dict = {}
        for attr in target_dataset_attributes.stream():
            target_attribute_dict[attr.name] = attr

        # Update attributes in dataset
        for attribute in attribute_specs:
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
        attribute_names = [attr.to_dict()["name"] for attr in attribute_specs]
        for existing_attribute in target_attribute_dict.keys():
            if (
                existing_attribute not in attribute_names
                and existing_attribute not in primary_keys
            ):
                target_dataset_attributes.delete_by_resource_id(
                    target_attribute_dict[existing_attribute].resource_id
                )

    return dataset


def exists(*, client: Client, dataset: str) -> bool:
    """Check if the dataset exists on target instance

    Args:
        target_instance: Tamr python client object for the target instance
        dataset: The dataset name

    Return:
        True or False for if the dataset exists in target instance
    """

    try:
        client.datasets.by_name(dataset)
    except KeyError:
        return False

    return True


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
    for idx in range(len(attribute_names)):
        name = attribute_names[idx]
        if attribute_types and name in attribute_types.keys():
            attr_type = AttributeType(attribute_types[name])
        else:
            attr_type = AttributeType(default_type)
        attribute_specs.append(AttributeSpec.new().with_name(name).with_type(attr_type.spec()))
    return attribute_specs
