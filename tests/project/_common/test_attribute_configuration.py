"""Tests for tasks related to transformations with Tamr projects"""
import pytest

from tamr_toolbox.project.mastering import attribute_configuration
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["attribute_configuration_editing"]


@mock_api()
def test_update_attribute_configuration_no_failures():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Test changes attribute between ml and non-ml

    # Set attribute to check
    attribute_non_ml_name = "attribute_change_ml"

    # Check project has attribute present
    project_attribute_configuration_prev = attribute_configuration.get_attribute_configurations(
        project=project
    )
    # Get the values from the configurations

    # Loop through all the attributes
    for attribute_config_prev_single in project_attribute_configuration_prev:
        # Get values from configuration spec
        attribute_config_prev_single_dict = attribute_config_prev_single.to_dict()
        # Check if name matches
        if attribute_config_prev_single_dict["attributeName"] == attribute_non_ml_name:
            # Save attribute info
            attribute_config_prev = attribute_config_prev_single_dict

    # Store whether ml enabled
    attribute_ml_enabled_prev = attribute_config_prev["enabledForMl"]

    # Change attribute configuration
    attribute_ml_enabled_change = not attribute_ml_enabled_prev

    # Update attribute configurations
    attribute_configuration.update_attribute_configuration(
        project=project,
        attribute_name=attribute_non_ml_name,
        enabled_for_ml=attribute_ml_enabled_change,
    )

    # Get attribute configuration again to check if updated correctly
    project_attribute_configuration_next = attribute_configuration.get_attribute_configurations(
        project=project
    )
    # Get the values from the configurations

    # Loop through all the attributes
    for attribute_config_next_single in project_attribute_configuration_next:
        # Get values from configuration spec
        attribute_config_next_single_dict = attribute_config_next_single.to_dict()
        # Check if name matches
        if attribute_config_next_single_dict["attributeName"] == attribute_non_ml_name:
            # Save attribute info
            attribute_config_next = attribute_config_next_single_dict

        # Store whether ml enabled
    attribute_ml_enabled_next = attribute_config_next["enabledForMl"]

    print(attribute_ml_enabled_prev)
    print(attribute_ml_enabled_next)

    # Change attribute configuration
    assert attribute_ml_enabled_next ^ attribute_ml_enabled_prev


@mock_api()
def test_update_attribute_configuration_runtime():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(RuntimeError):
        attribute_configuration.update_attribute_configuration(project=project, attribute_name="")


@mock_api()
def test_update_attribute_configuration_no_values():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(ValueError):
        attribute_configuration.update_attribute_configuration(
            project=project, attribute_name="attribute_1"
        )


@mock_api()
def test_update_attribute_configuration_invalid_attribute_role():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(ValueError):
        attribute_configuration.update_attribute_configuration(
            project=project, attribute_name="attribute_1", attribute_role="invalid"
        )


@mock_api()
def test_update_attribute_configuration_invalid_similarity_function():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(ValueError):
        attribute_configuration.update_attribute_configuration(
            project=project, attribute_name="attribute_1", similarity_function="invalid"
        )


@mock_api()
def test_update_attribute_configuration_invalid_enabled_for_ml():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(ValueError):
        attribute_configuration.update_attribute_configuration(
            project=project, attribute_name="attribute_1", enabled_for_ml="invalid"
        )


@mock_api()
def test_update_attribute_configuration_invalid_tokenizer():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    with pytest.raises(ValueError):
        attribute_configuration.update_attribute_configuration(
            project=project, attribute_name="attribute_1", tokenizer="invalid"
        )
