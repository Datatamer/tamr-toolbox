"""Tests for asks related to running jobs for Tamr Mastering projects"""
import pytest

from tamr_toolbox.project import mastering
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_mastering"]


@mock_api()
def test_run():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    project = project.as_mastering()
    all_ops = mastering.jobs.run(project)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 6

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Update Pairs" == all_ops[1].description
    assert "Predict Pairs" == all_ops[2].description
    assert "Generate High-impact Pairs" == all_ops[3].description
    assert "Clustering" == all_ops[4].description
    assert "Publish clusters" == all_ops[5].description


@mock_api()
def test_run_with_feedback_and_estimate():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    project = project.as_mastering()
    all_ops = mastering.jobs.run(project, run_apply_feedback=True, run_estimate_pair_counts=True)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 8

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert (
        all_ops[1].description == "Generate Pair Estimates"
        or len(all_ops[1].status["message"]) == 0  # Indicating a 204, already up to date
    )
    assert "Update Pairs" == all_ops[2].description
    assert "Train Mastering Model" == all_ops[3].description
    assert "Predict Pairs" == all_ops[4].description
    assert "Generate High-impact Pairs" == all_ops[5].description
    assert "Clustering" == all_ops[6].description
    assert "Publish clusters" == all_ops[7].description


@mock_api()
def test_run_with_update_realtime_match():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    project = project.as_mastering()
    all_ops = mastering.jobs.run(project, run_update_realtime_match=True)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 7

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Update Pairs" == all_ops[1].description
    assert "Predict Pairs" == all_ops[2].description
    assert "Generate High-impact Pairs" == all_ops[3].description
    assert "Clustering" == all_ops[4].description
    assert "Publish clusters" == all_ops[5].description
    assert "Update LLM datasets" == all_ops[6].description

    # Test updating match data when it's already up-to-date
    ops = mastering.jobs._run_custom(project, run_update_realtime_match=True)
    assert len(ops) == 1
    op = ops[0]

    assert op.succeeded()
    assert "already up-to-date." in op.description


@mock_api()
@pytest.mark.parametrize(
    "project_id",
    [
        (CONFIG["projects"]["minimal_golden_records"]),
        (CONFIG["projects"]["minimal_categorization"]),
        (CONFIG["projects"]["minimal_schema_mapping"]),
    ],
)
def test_run_with_wrong_project_type(project_id: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(project_id)

    with pytest.raises(TypeError):
        mastering.jobs.run(project)


@mock_api()
def test_safe_generate_pairs():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    estimate_project_id = CONFIG["projects"]["first_pairs_estimate"]
    project = client.projects.by_resource_id(estimate_project_id)
    project = project.as_mastering()

    op = utils.operation.safe_estimate_counts(project)
    assert op.succeeded()

    count = project.client.get(
        f"/api/versioned/v1/projects/{estimate_project_id}/estimatedPairCounts"
    ).json()
    assert int(count["totalEstimate"]["candidatePairCount"]) >= int(
        count["totalEstimate"]["generatedPairCount"]
    )
