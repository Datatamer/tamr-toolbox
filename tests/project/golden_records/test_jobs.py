"""Tests for tasks related to running jobs for Tamr Golden Records projects"""
import pytest

from tamr_toolbox.project import golden_records
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_golden_records"]


@mock_api()
def test_run():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(PROJECT_ID)
    all_ops = golden_records.jobs.run(project)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 2

    assert "Updating Golden Records" == all_ops[0].description
    assert "Updating published datasets for Golden Records" == all_ops[1].description


@mock_api()
@pytest.mark.parametrize(
    "project_id",
    [
        (CONFIG["projects"]["minimal_categorization"]),
        (CONFIG["projects"]["minimal_mastering"]),
        (CONFIG["projects"]["minimal_schema_mapping"]),
    ],
)
def test_run_with_wrong_project_type(project_id: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(project_id)

    with pytest.raises(TypeError):
        golden_records.jobs.run(project)
