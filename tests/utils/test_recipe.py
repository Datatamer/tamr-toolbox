"""Tests for tasks related to getting recipe info"""
import tamr_toolbox as tbox
from tests._common import get_toolbox_root_dir
from tamr_toolbox.utils.testing import mock_api

CONFIG = tbox.utils.config.from_yaml(
    f"{get_toolbox_root_dir()}/tests/mocking/resources/toolbox_test.yaml"
)
MASTERING_PROJECT_ID = CONFIG["projects"]["minimal_mastering"]


@mock_api()
def test_get_dedup_recipe_id():
    client = tbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(MASTERING_PROJECT_ID).as_mastering()
    expected_dedup_recipe_id = "3"
    returned_dedup_recipe_id = tbox.utils.recipe.get_dedup_recipe_id(project)
    assert returned_dedup_recipe_id == expected_dedup_recipe_id
