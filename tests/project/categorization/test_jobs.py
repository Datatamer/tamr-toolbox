"""Tests for tasks related to running jobs for Tamr Categorization projects"""
import pytest

from tamr_toolbox.project import categorization
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_categorization"]


@mock_api()
def test_run():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    project = project.as_categorization()
    all_ops = categorization.jobs.run(project)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 2

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Predict record categorizations" == all_ops[1].description


@mock_api()
def test_run_with_feedback():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    project = project.as_categorization()
    all_ops = categorization.jobs.run(project, run_apply_feedback=True)

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
@pytest.mark.parametrize(
    "project_id",
    [
        (CONFIG["projects"]["minimal_golden_records"]),
        (CONFIG["projects"]["minimal_mastering"]),
        (CONFIG["projects"]["minimal_schema_mapping"]),
    ],
)
def test_run_with_wrong_project_type(project_id: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(project_id)

    with pytest.raises(TypeError):
        categorization.jobs.run(project)
