"""Tests for tasks related to running jobs for Tamr Schema Mapping projects"""
import pytest

from tamr_toolbox.project import schema_mapping
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir


CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_schema_mapping"]


@mock_api()
def test_run():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    all_ops = schema_mapping.jobs.run(project)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 1

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )


@mock_api()
@pytest.mark.parametrize(
    "project_id",
    [
        (CONFIG["projects"]["minimal_golden_records"]),
        (CONFIG["projects"]["minimal_mastering"]),
        (CONFIG["projects"]["minimal_categorization"]),
    ],
)
def test_run_with_wrong_project_type(project_id: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(project_id)

    with pytest.raises(TypeError):
        schema_mapping.jobs.run(project)
