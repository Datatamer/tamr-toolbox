from tamr_unify_client import Client
from tamr_client.dataset._dataset import create as create_new_dataset
from tamr_unify_client.dataset.resource import Dataset
from typing import List, Optional, Tuple
from attribute_type import ArrayType, AttributeType

def create_maybe(tamr: Client, 
    dataset_name: str, 
    attributes: List[str],
    primary_keys: Tuple[str, ...],
    attribute_types: Optional[List[AttributeType]] = None,
    description: Optional[str] = None,
    external_id: Optional[str] = None
    ) -> Dataset:
    """Flexibly create/update a source dataset in Tamr.
        Args:
            tamr: Tamr instance
            dataset_name: Dataset name
            attributes: List of all attribute names 
            primary_keys: Dataset primary key attribute names
            attribute_types: List of attribute type objects
            description: Dataset description
            external_id: External ID of the dataset
        Returns:
            Dataset created in Tamr
        Raises:
            requests.HTTPError: If any HTTP error is encountered
            ValueError: trying to alter a unified dataset
            ValueError: primary key is not an attribute of the data
            ValueError: Number of attributes does not match number of attribute types
    """
    #this isn't a totally valid check
    if "unified_dataset" in dataset_name:
        raise ValueError(f"{dataset_name} is a unified dataset")

    for primary_key in primary_keys:
        if primary_key not in attributes:
            raise ValueError(f"{primary_key} is not an attribute of the data")

    if attribute_types and len(attribute_types) != len(attributes):
        raise ValueError(f"Number of attributes does not match number of attribute types")
 
    dataset_exists = _check_dataset_exists(target_instance=tamr, dataset=dataset_name)
    if not dataset_exists:
        create_new_dataset(instance = tamr, name = dataset_name, key_attribute_names= primary_keys, description=description, external_id=external_id)

    target_dataset = tamr.datasets.by_name(dataset_name)

    # Update attributes in dataset
    target_dataset_attributes = target_dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr

    for idx in range(len(attributes)):
        attribute_name = attributes[idx]
        if attribute_name in target_attribute_dict.keys():
            # this attribute already exists
            # TC only has option to update description (not type)
            continue

        # use passed in attribute types if possible, otherwise default to array str
        if attribute_types:
            new_attribute_type = attribute_types[idx]
        else:
            new_attribute_type = ArrayType("STRING")

        attr_spec = {
            "name": attribute_name,
            "type": new_attribute_type,
        }
        target_dataset_attributes.create(attr_spec)
    
    #remove any attributes from dataset that aren't in the new list of attributes
    for existing_attribute in target_attribute_dict.keys():
        if existing_attribute not in attributes:
            target_dataset_attributes.delete(target_attribute_dict[existing_attribute])
            # delete(session=tamr, attribute=target_attribute_dict[existing_attribute])


def _check_dataset_exists(
    *, target_instance: Client, dataset: str
    ) -> bool:
    """ Check if the dataset exists on target instance
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
