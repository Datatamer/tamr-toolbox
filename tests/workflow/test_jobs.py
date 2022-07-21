"""Tests for tasks related to running jobs for groups of Tamr projects"""
import pytest

from tamr_unify_client.project.resource import Project

from tamr_toolbox import workflow
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_run_one():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    all_ops = workflow.jobs.run([project])

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 2

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Predict record categorizations" == all_ops[1].description


@mock_api()
def test_run_one_training():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    all_ops = workflow.jobs.run([project], run_apply_feedback=True)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 3

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert (
        f"materialize {project.unified_dataset().name}_classification_model"
        == all_ops[1].description
    )
    assert "Predict record categorizations" == all_ops[2].description


@mock_api()
def test_run_many():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project1 = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    project2 = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    project3 = client.projects.by_resource_id(CONFIG["projects"]["minimal_golden_records"])
    all_ops = workflow.jobs.run([project1, project2, project3])

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 10

    # Schema mapping
    assert (
        f"Materialize views [{project1.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )

    # Mastering
    assert (
        f"Materialize views [{project2.unified_dataset().name}] to Elastic"
        == all_ops[1].description
    )
    assert "Update Pairs" == all_ops[2].description
    assert "Predict Pairs" == all_ops[3].description
    assert "Generate High-impact Pairs" == all_ops[4].description
    assert "Clustering" == all_ops[5].description
    assert "Publish clusters" == all_ops[6].description

    # Golden records
    assert "Updating all profiling information for Golden Records" == all_ops[7].description
    assert "Updating Golden Records" == all_ops[8].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[9].description


@mock_api()
def test_run_many_training():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project1 = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    project2 = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    project3 = client.projects.by_resource_id(CONFIG["projects"]["minimal_golden_records"])
    all_ops = workflow.jobs.run(
        [project1, project2, project3], run_apply_feedback=True, run_estimate_pair_counts=True
    )

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 12

    # Schema mapping
    assert (
        f"Materialize views [{project1.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )

    # Mastering
    assert (
        f"Materialize views [{project2.unified_dataset().name}] to Elastic"
        == all_ops[1].description
    )
    assert (
        all_ops[2].description == "Generate Pair Estimates"
        or len(all_ops[2].status["message"]) == 0  # Indicating a 204, already up to date
    )
    assert "Update Pairs" == all_ops[3].description
    assert "Train Mastering Model" == all_ops[4].description
    assert "Predict Pairs" == all_ops[5].description
    assert "Generate High-impact Pairs" == all_ops[6].description
    assert "Clustering" == all_ops[7].description
    assert "Publish clusters" == all_ops[8].description

    # Golden records
    assert "Updating all profiling information for Golden Records" == all_ops[9].description
    assert "Updating Golden Records" == all_ops[10].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[11].description


@mock_api()
def test_run_error():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = Project.from_json(client, resource_json={"name": "fake", "type": "NOT_REAL"})

    with pytest.raises(KeyError):
        workflow.jobs.run([project])
