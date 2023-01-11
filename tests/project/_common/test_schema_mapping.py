"""Tests for tasks related to schema mapping as part of Tamr projects"""
import logging
from json import JSONDecodeError
from unittest.mock import patch

import pytest
from tamr_unify_client.project.attribute_mapping.collection import AttributeMappingCollection

from tamr_toolbox import utils
from tamr_toolbox.project.mastering import schema
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_empty_variables_raises_runtime():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    with pytest.raises(ValueError):
        schema.map_attribute(
            test_project,
            source_attribute_name="",
            source_dataset_name="people_tiny.csv",
            unified_attribute_name="first_name",
        )

    with pytest.raises(ValueError):
        schema.map_attribute(
            test_project,
            source_attribute_name="first_name",
            source_dataset_name="people_tiny.csv",
            unified_attribute_name="",
        )

    with pytest.raises(ValueError):
        schema.map_attribute(
            test_project,
            source_attribute_name="first_name",
            source_dataset_name="",
            unified_attribute_name="id",
        )

    with pytest.raises(ValueError):
        schema.map_attribute(
            test_project,
            source_attribute_name="",
            source_dataset_name="",
            unified_attribute_name="",
        )


@mock_api()
def test_not_found_errors():

    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    with pytest.raises(ValueError, match="Dataset nonexistent_dataset.csv not found"):
        schema.map_attribute(
            test_project,
            source_attribute_name="test_attribute",
            source_dataset_name="nonexistent_dataset.csv",
            unified_attribute_name="test_unified_attribute",
        )

    with pytest.raises(ValueError, match="Attribute nonexistent_attribute not found in"):
        schema.map_attribute(
            test_project,
            source_attribute_name="nonexistent_attribute",
            source_dataset_name="people_tiny.csv",
            unified_attribute_name="all_names",
        )
    return


@mock_api()
def test_map_and_unmap_attribute():
    """tests the mapping of an attribute. 'first_name' in 'people_tiny.csv' to 'all_names' in the
    minimal_schema_mapping project. Checks that the mappings don't contain that mapping to start,
    then maps, and asserts that it has that mapping"""
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    # make sure the mapping doesn't exist to start with
    initial_mappings = test_project.attribute_mappings()
    for mapping in initial_mappings.stream():
        # make sure there is no mapping of 'first_name' to 'all_names'
        spec = mapping.spec().to_dict()
        source_attr_name = spec["inputAttributeName"]
        source_dataset = spec["inputDatasetName"]
        unified_attr_name = spec["unifiedAttributeName"]
        if (
            source_attr_name == "first_name"
            and source_dataset == "people_tiny.csv"
            and unified_attr_name == "all_names"
        ):
            raise RuntimeError("mapping to be tested already exists!")

    # now map first_name to all_names
    mapping = schema.map_attribute(
        test_project,
        source_attribute_name="first_name",
        source_dataset_name="people_tiny.csv",
        unified_attribute_name="all_names",
    )

    # ensure mapping is in the project's collection now
    updated_mappings = [x.spec().to_dict() for x in test_project.attribute_mappings().stream()]
    # have to convert to dict for python to do equality comparison correctly
    assert mapping.spec().to_dict() in updated_mappings

    # MAPPING ATTRIBUTE PASSED - now check unmap
    schema.unmap_attribute(
        test_project,
        source_attribute_name="first_name",
        source_dataset_name="people_tiny.csv",
        unified_attribute_name="all_names",
    )

    final_mappings = [x.spec().to_dict() for x in test_project.attribute_mappings().stream()]

    assert mapping.spec().to_dict() not in final_mappings


@mock_api()
def test_unmap_nonexistent_attribute(caplog):

    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    src, dataset, unif = "first_name", "people_tiny.csv", "nonexistent_attribute"

    with caplog.at_level(logging.WARNING):
        schema.unmap_attribute(
            test_project,
            source_attribute_name=src,
            source_dataset_name=dataset,
            unified_attribute_name=unif,
        )

        message = f"Mapping of {src} in dataset {dataset} to unified attribute {unif} not found!"
        assert message in caplog.text
    return


@mock_api()
def test_already_mapped():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    # now map first_name to all_names
    initial_mapping = schema.map_attribute(
        test_project,
        source_attribute_name="first_name",
        source_dataset_name="people_tiny.csv",
        unified_attribute_name="all_names",
    )

    # now map a second time and assert it:
    # 1) doesn't break and
    # 2) returns the same mapping as above - again have to convert to dict for python equality
    # to work
    assert (
        initial_mapping.spec().to_dict()
        == schema.map_attribute(
            test_project,
            source_attribute_name="first_name",
            source_dataset_name="people_tiny.csv",
            unified_attribute_name="all_names",
        )
        .spec()
        .to_dict()
    )

    # now unmap to revert state
    schema.unmap_attribute(
        test_project,
        source_attribute_name="first_name",
        source_dataset_name="people_tiny.csv",
        unified_attribute_name="all_names",
    )


@mock_api()
def test_bootstrap_dataset_not_in_project_dont_add():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_name("groceries_tiny.csv")
    project = client.projects.by_name("minimal_schema_mapping")
    with pytest.raises(RuntimeError):
        schema.bootstrap_dataset(project, source_dataset=source_dataset)


@mock_api()
def test_bootstrap_dataset_not_in_project_and_add():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_name("groceries_tiny.csv")
    project = client.projects.by_name("minimal_schema_mapping")
    # map the dataset
    mappings = schema.bootstrap_dataset(
        project, source_dataset=source_dataset, force_add_dataset_to_project=True
    )

    # from mappings make a tuple of (source_attr, unified_attr)
    mapping_tuples = []
    for mapping in mappings:
        mapping_dict = mapping.spec().to_dict()
        source_attr = mapping_dict["inputAttributeName"]
        unified_attr = mapping_dict["unifiedAttributeName"]
        # add all mappings to ensure only bootstrapping happened
        mapping_tuples.append((source_attr, unified_attr))

    # now create expected mappings
    expected_tuples = [(x.name, x.name) for x in source_dataset.attributes]

    assert expected_tuples == mapping_tuples


@mock_api()
def test_unmapping_dataset_and_remove():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_name("groceries_tiny.csv")
    project = client.projects.by_name("minimal_schema_mapping")

    # first add and bootstrap
    schema.bootstrap_dataset(
        project, source_dataset=source_dataset, force_add_dataset_to_project=True
    )

    # now  unmap the dataset
    schema.unmap_dataset(project, source_dataset=source_dataset, remove_dataset_from_project=True)

    # get mappings and assert there are none with source dataset 'groceries_tiny.csv'
    updated_mappings = project.attribute_mappings()
    assert source_dataset.name not in [x.input_dataset_name for x in updated_mappings.stream()]

    # assert that the dataset is no longer in the project
    assert source_dataset.name not in [x.name for x in project.input_datasets()]


@mock_api()
def test_unmap_unrelated_dataset(caplog):

    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_name("groceries_tiny.csv")
    project = client.projects.by_name("minimal_schema_mapping")

    with caplog.at_level(logging.WARNING):
        schema.unmap_dataset(project, source_dataset=source_dataset, skip_if_missing=True)
        assert "However skip_if_missing flag is set so will do nothing" in caplog.text

    with pytest.raises(RuntimeError, match="and skip_if_missing not set to True so failing!"):
        schema.unmap_dataset(project, source_dataset=source_dataset)

    return


@mock_api()
@patch.object(AttributeMappingCollection, "stream", new=lambda cls: [])
def test_uncaught_jsonencode_error():

    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name("minimal_schema_mapping")

    with pytest.raises(JSONDecodeError):
        schema.map_attribute(
            test_project,
            source_attribute_name="first_name",
            source_dataset_name="people_tiny.csv",
            unified_attribute_name="first_name",
        )
    return
