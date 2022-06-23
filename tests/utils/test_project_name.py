"""Tests for tasks related to Tamr project name"""
import pytest
from tamr_toolbox import utils
from tamr_toolbox.utils._project_name import _get_original_project_name
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_bad_project():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    with pytest.raises(RuntimeError, match="Unable to retrieve project data"):
        _get_original_project_name(client, project_id=1000000)
    return None
