"""Tests for tasks related to transformations with Tamr projects"""
import pytest

from tamr_toolbox.project import categorization, mastering, schema_mapping
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_categorization"]


@mock_api()
def test_input_and_unified_transformations():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Should start with no tx
    initial_tx = categorization.transformations.get_all(project)

    assert len(initial_tx.unified_scope) == 0
    assert len(initial_tx.input_scope) == 0

    input_tx1 = categorization.transformations.InputTransformation(
        transformation="SELECT *, 1 as one;"
    )
    input_dataset = next(project.input_datasets().stream())
    input_tx2 = categorization.transformations.InputTransformation(
        transformation="//comment\nSELECT *, 2 as two;", datasets=[input_dataset]
    )

    unified_tx1 = "//comment\nSELECT *, 3 as three;"

    new_tx = categorization.transformations.TransformationGroup(
        input_scope=[input_tx1, input_tx2], unified_scope=[unified_tx1]
    )

    # replace current tx and get them again
    categorization.transformations.set_all(project, new_tx)
    updated_tx = categorization.transformations.get_all(project)

    assert len(updated_tx.input_scope) == 2
    assert updated_tx.input_scope[0].transformation == "SELECT *, 1 as one;"
    assert len(updated_tx.input_scope[0].datasets) == 0
    assert updated_tx.input_scope[1].transformation == "//comment\nSELECT *, 2 as two;"
    assert len(updated_tx.input_scope[1].datasets) == 1
    assert updated_tx.input_scope[1].datasets[0].name == input_dataset.name

    assert len(updated_tx.unified_scope) == 1
    assert updated_tx.unified_scope[0] == "//comment\nSELECT *, 3 as three;"

    # Return project to original state
    categorization.transformations.set_all(project, initial_tx)
    categorization.jobs.run(project.as_categorization())


@mock_api()
def test_transformations_value_error():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Linting error
    with pytest.raises(ValueError):
        bad_tx = categorization.transformations.TransformationGroup(
            unified_scope=["SELECT *, fake_column;"]
        )
        categorization.transformations.set_all(project, bad_tx)

    # Bad input dataset
    with pytest.raises(ValueError):
        bad_tx = categorization.transformations.TransformationGroup(
            input_scope=[
                categorization.transformations.InputTransformation(
                    "SELECT *;", [project.unified_dataset()]
                )
            ]
        )
        categorization.transformations.set_all(project, bad_tx)


@mock_api()
def test_input_and_unified_transformations_overwrite_flag():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Should start with no tx
    initial_tx = categorization.transformations.get_all(project)

    assert len(initial_tx.unified_scope) == 0
    assert len(initial_tx.input_scope) == 0

    # Should be able to set without overwrite
    categorization.transformations.set_all(
        project,
        categorization.transformations.TransformationGroup(unified_scope=["Select *;"]),
        allow_overwrite=False,
    )
    # Should now have 1 unified
    tx_after_write = categorization.transformations.get_all(project)
    assert len(tx_after_write.unified_scope) == 1
    assert len(tx_after_write.input_scope) == 0

    with pytest.raises(RuntimeError):
        # Should not be able to set without overwrite
        categorization.transformations.set_all(
            project,
            categorization.transformations.TransformationGroup(
                unified_scope=["Select 'a' AS example;"]
            ),
            allow_overwrite=False,
        )

    # Return project to original state
    categorization.transformations.set_all(project, initial_tx)
    categorization.jobs.run(project.as_categorization())


@mock_api()
def test_unified_transformations():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Should start with no tx
    initial_tx = categorization.transformations.get_all(project)

    assert len(initial_tx.unified_scope) == 0
    assert len(initial_tx.input_scope) == 0

    unified_tx = ["SELECT *;", "//comment\nSELECT *, 3 as three;"]

    # replace current tx and get them again
    categorization.transformations.set_all_unified(project, unified_tx)
    updated_tx = categorization.transformations.get_all(project)

    assert len(updated_tx.input_scope) == 0

    assert len(updated_tx.unified_scope) == 2
    assert updated_tx.unified_scope[0] == unified_tx[0]
    assert updated_tx.unified_scope[1] == unified_tx[1]

    updated_ud_tx = categorization.transformations.get_all_unified(project)
    assert len(updated_ud_tx) == 2
    assert updated_ud_tx[0] == unified_tx[0]
    assert updated_ud_tx[1] == unified_tx[1]

    # Return project to original state
    categorization.transformations.set_all(project, initial_tx)
    categorization.jobs.run(project.as_categorization())


@mock_api()
def test_unified_transformations_overwrite_flag():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)

    # Should start with no tx
    initial_tx = categorization.transformations.get_all(project)

    assert len(initial_tx.unified_scope) == 0
    assert len(initial_tx.input_scope) == 0

    # Add an input TX
    input_tx1 = categorization.transformations.InputTransformation(
        transformation="SELECT *, 1 as one;"
    )
    categorization.transformations.set_all(
        project, categorization.transformations.TransformationGroup(input_scope=[input_tx1])
    )

    ud_tx = ["SELECT *, 3 AS three;"]

    # Should be able to set without overwrite, even though there is an input TX
    categorization.transformations.set_all_unified(project, ud_tx, allow_overwrite=False)

    # Should now have 1 unified
    tx_after_write = categorization.transformations.get_all(project)
    assert len(tx_after_write.unified_scope) == 1
    assert len(tx_after_write.input_scope) == 1

    with pytest.raises(RuntimeError):
        # Should not be able to set without overwrite
        categorization.transformations.set_all_unified(
            project, ["Select 'a' AS example;"], allow_overwrite=False
        )

    # Return project to original state
    categorization.transformations.set_all(project, initial_tx)
    categorization.jobs.run(project.as_categorization())


def test_transformations_aliases():
    """Test the transformations code is identical in all 3 project types"""
    assert categorization.transformations == mastering.transformations
    assert categorization.transformations == schema_mapping.transformations
