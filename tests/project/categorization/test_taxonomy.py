"""Tests for tasks related to managing the taxonomy for Tamr Categorization projects"""

from tamr_toolbox.project import categorization
from tamr_toolbox import utils
import json

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
PROJECT_ID = CONFIG["projects"]["minimal_categorization"]


@mock_api()
def test_get_taxonomy_as_dataframe():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    df = categorization.taxonomy.get_taxonomy_as_dataframe(client, PROJECT_ID)

    assert df.shape == (9, 3)


@mock_api()
def test_create_node():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    new_node_path = ["Dairy", "Test-Cheese"]

    categorization.taxonomy.create_node(client, PROJECT_ID, new_node_path)
    cat_response = client.get(f"projects/{PROJECT_ID}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == new_node_path]

    assert len(target_cat) == 1
    assert target_cat[0]["path"] == new_node_path


@mock_api()
def test_rename_node():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    node_path = ["Dairy", "Test-Cheese"]
    new_name = "Test Cheese"
    new_path = ["Dairy", "Test Cheese"]

    categorization.taxonomy.rename_node(client, PROJECT_ID, new_name, node_path)
    cat_response = client.get(f"projects/{PROJECT_ID}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == new_path]

    assert len(target_cat) == 1
    assert target_cat[0]["path"] == new_path


@mock_api()
def test_move_node():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    node_path = ["Dairy", "Test Cheese"]
    new_path = ["Dairy", "Cheese", "Test Cheese"]

    categorization.taxonomy.move_node(client, PROJECT_ID, node_path, new_path)
    cat_response = client.get(f"projects/{PROJECT_ID}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == new_path]

    assert len(target_cat) == 1
    assert target_cat[0]["path"] == new_path

    old_cat = [cat for cat in all_cats if cat["path"] == node_path]
    assert len(old_cat) == 0


@mock_api()
def test_delete_node():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    node_path = ["Dairy", "Cheese", "Test Cheese"]

    categorization.taxonomy.delete_node(client, PROJECT_ID, node_path)
    cat_response = client.get(f"projects/{PROJECT_ID}/taxonomy/categories")
    all_cats = json.loads(cat_response.content)
    target_cat = [cat for cat in all_cats if cat["path"] == node_path]

    assert len(target_cat) == 0
    df = categorization.taxonomy.get_taxonomy_as_dataframe(client, PROJECT_ID)
    assert df.shape == (9, 3)
