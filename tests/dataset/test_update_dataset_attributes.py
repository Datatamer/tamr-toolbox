"""Tests for tasks related creating and updating datasets in Tamr"""
import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api

from tests._common import get_toolbox_root_dir

# A valid password is not needed for offline tests, some value must be provided
CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
DATASET_NAME = "test_create_dataset"
PRIMARY_KEYS = ["unique_id"]


# Note that some test cases are dependant on previous ones
# So all tests must be set to enforce_online_test = True or all to False
enforce_online_test = False


@mock_api(enforce_online_test=enforce_online_test)
def test_update_attribute_description():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attribute_name = "user_id"
    attribute_description = {attribute_name: "The unique id for each sales rep"}

    tbox.dataset.manage.edit_attributes(
        dataset=dataset, attributes=[attribute_name], attribute_descriptions=attribute_description,
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    target_dataset_attributes = updated_dataset.attributes
    target_attribute_dict = {}
    for attr in target_dataset_attributes.stream():
        target_attribute_dict[attr.name] = attr

    updated_attr = target_attribute_dict[attribute_name]
    assert updated_attr.description == attribute_description[attribute_name]


@mock_api(enforce_online_test=enforce_online_test)
def test_remove_attribute_by_name():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_name(DATASET_NAME)
    attribute_name = "user_id"

    tbox.dataset.manage.delete_attributes(
        dataset=dataset, attributes=[attribute_name],
    )

    updated_dataset = client.datasets.by_name(DATASET_NAME)
    target_dataset_attributes = updated_dataset.attributes
    updated_attributes = []
    for attr in target_dataset_attributes.stream():
        updated_attributes.append(attr.name)

    assert attribute_name not in updated_attributes
