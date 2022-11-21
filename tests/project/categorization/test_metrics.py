"""Tests for tasks related to calculating metrics for Tamr Categorization projects"""
import math
import pytest

import tamr_toolbox.data_io.dataframe as dataframe
from tamr_toolbox.project import categorization
from tamr_toolbox.project.categorization import metrics
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

CATEGORIZATION_DATASET_ID = CONFIG["datasets"][
    "minimal_categorization_unified_dataset_classifications_average_confidences"
]
CATEGORIZATION_PROJECT_ID = CONFIG["projects"]["minimal_categorization"]
MASTERING_PROJECT_ID = CONFIG["projects"]["minimal_mastering"]
INPUT_DATASET_ID = CONFIG["datasets"]["groceries_tiny"]


@mock_api()
def test_check_dataset_with_confidence_invalid_attributes():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(INPUT_DATASET_ID)

    with pytest.raises(RuntimeError):
        metrics._check_dataset_with_confidence(dataset)


@mock_api()
def test_check_invalid_taxonomy_depth():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)

    with pytest.raises(ValueError):
        metrics._check_taxonomy_depth(project, tier=4)


@mock_api()
def test_check_taxonomy_depth_true():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)

    metrics._check_taxonomy_depth(project, tier=1)
    metrics._check_taxonomy_depth(project, tier=2)
    metrics._check_taxonomy_depth(project, tier=3)


@mock_api()
def test__create_leaf_node_set():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    taxonomy = project.as_categorization().taxonomy()

    leaf_set = metrics._create_leaf_node_set(taxonomy)
    assert len(leaf_set) == 6
    assert "Dairy|Cheese" in leaf_set
    assert "Dairy|Milk" in leaf_set
    assert "Meat|Beef" in leaf_set
    assert "Meat|Chicken|boneless" in leaf_set
    assert "Meat|Chicken|bone-in" in leaf_set
    assert "Vegetables" in leaf_set


@mock_api()
def test_get_categories_at_tier_1():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    category_list = metrics._get_categories_at_tier(project, tier=1)

    assert len(category_list) == 3
    assert "Dairy" in category_list
    assert "Meat" in category_list
    assert "Vegetables" in category_list


@mock_api()
def test_get_categories_at_tier_2():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    category_set = metrics._get_categories_at_tier(project, tier=2)

    assert len(category_set) == 4
    assert "Dairy|Cheese" in category_set
    assert "Dairy|Milk" in category_set
    assert "Meat|Beef" in category_set
    assert "Meat|Chicken" in category_set


@mock_api()
def test_get_categories_at_tier_leaf():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    category_set = metrics._get_categories_at_tier(project, tier=-1)

    assert len(category_set) == 6
    assert "Dairy|Cheese" in category_set
    assert "Dairy|Milk" in category_set
    assert "Meat|Beef" in category_set
    assert "Meat|Chicken|boneless" in category_set
    assert "Meat|Chicken|bone-in" in category_set
    assert "Vegetables" in category_set


@mock_api()
def test_get_dataset_with_confidence():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    dataset = metrics._get_dataset_with_confidence(project)

    assert (
        dataset.name
        == "minimal_categorization_unified_dataset_classifications_average_confidences"
    )


@mock_api()
def test_get_dataset_with_confidence_invalid_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(MASTERING_PROJECT_ID)

    with pytest.raises(RuntimeError):
        metrics._get_dataset_with_confidence(project)


@mock_api()
def test_extract_confidence_tier1():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    dataset = client.datasets.by_resource_id(CATEGORIZATION_DATASET_ID)
    category_set = metrics._get_categories_at_tier(project, tier=1)
    tier1_confidence_dict = metrics._extract_confidence(dataset=dataset, category_set=category_set)

    assert len(tier1_confidence_dict) == 3
    assert math.isclose(tier1_confidence_dict["Dairy"], 0.81, rel_tol=0.01)
    assert math.isclose(tier1_confidence_dict["Meat"], 0.64, rel_tol=0.01)
    assert tier1_confidence_dict["Vegetables"] is None


@mock_api()
def test_extract_confidence_leaf():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    dataset = client.datasets.by_resource_id(CATEGORIZATION_DATASET_ID)
    category_set = metrics._get_categories_at_tier(project, tier=-1)
    leaf_confidence_dict = metrics._extract_confidence(dataset=dataset, category_set=category_set)

    assert len(leaf_confidence_dict) == 6
    assert math.isclose(leaf_confidence_dict["Dairy|Cheese"], 0.77, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Dairy|Milk"], 0.92, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Beef"], 0.81, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Chicken|bone-in"], 0.53, rel_tol=0.01)
    assert leaf_confidence_dict["Meat|Chicken|boneless"] is None
    assert leaf_confidence_dict["Vegetables"] is None


@mock_api()
def test_get_tier_confidence_default():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    default_node_confidence_dict = metrics.get_tier_confidence(project)

    assert len(default_node_confidence_dict) == 6
    assert math.isclose(default_node_confidence_dict["Dairy|Cheese"], 0.77, rel_tol=0.01)
    assert math.isclose(default_node_confidence_dict["Dairy|Milk"], 0.92, rel_tol=0.01)
    assert math.isclose(default_node_confidence_dict["Meat|Beef"], 0.81, rel_tol=0.01)
    assert math.isclose(default_node_confidence_dict["Meat|Chicken|bone-in"], 0.53, rel_tol=0.01)
    assert default_node_confidence_dict["Meat|Chicken|boneless"] is None
    assert default_node_confidence_dict["Vegetables"] is None


@mock_api()
def test_get_tier_confidence_tier1():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    tier1_confidence_dict = metrics.get_tier_confidence(project, tier=1)

    assert len(tier1_confidence_dict) == 3
    assert math.isclose(tier1_confidence_dict["Dairy"], 0.81, rel_tol=0.01)
    assert math.isclose(tier1_confidence_dict["Meat"], 0.64, rel_tol=0.01)
    assert tier1_confidence_dict["Vegetables"] is None


@mock_api()
def test_get_tier_confidence_tier3():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    tier3_confidence_dict = metrics.get_tier_confidence(project, tier=3)

    assert len(tier3_confidence_dict) == 2
    assert math.isclose(tier3_confidence_dict["Meat|Chicken|bone-in"], 0.53, rel_tol=0.01)
    assert tier3_confidence_dict["Meat|Chicken|boneless"] is None


@mock_api()
def test_get_tier_confidence_leaf():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    leaf_confidence_dict = metrics.get_tier_confidence(project, tier=-1)

    assert len(leaf_confidence_dict) == 6
    assert math.isclose(leaf_confidence_dict["Dairy|Cheese"], 0.77, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Dairy|Milk"], 0.92, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Beef"], 0.81, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Chicken|bone-in"], 0.53, rel_tol=0.01)
    assert leaf_confidence_dict["Meat|Chicken|boneless"] is None
    assert leaf_confidence_dict["Vegetables"] is None


@mock_api()
def test_get_tier_confidence_invalid_project_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(MASTERING_PROJECT_ID)

    with pytest.raises(TypeError):
        metrics.get_tier_confidence(project)


@mock_api()
def test_get_tier_confidence_invalid_tier_float():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    with pytest.raises(TypeError):
        metrics.get_tier_confidence(project, tier=1.5)


@mock_api()
def test_get_tier_confidence_invalid_tier_zero():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    with pytest.raises(ValueError):
        metrics.get_tier_confidence(project, tier=0)


@mock_api()
def test_get_tier_confidence_invalid_tier_less_than_negative_one():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)

    with pytest.raises(ValueError):
        metrics.get_tier_confidence(project, tier=-2)


@mock_api()
def test_get_tier_confidence_refresh():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(INPUT_DATASET_ID)
    df_input = dataframe.from_dataset(dataset)
    dataset.upsert_records(
        df_input.head(1).to_dict(orient="records"), primary_key_name=dataset.key_attribute_names[0]
    )
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)
    tier1_confidence_dict = metrics.get_tier_confidence(
        project, tier=1, allow_dataset_refresh=True
    )

    assert len(tier1_confidence_dict) == 3
    assert math.isclose(tier1_confidence_dict["Dairy"], 0.81, rel_tol=0.01)
    assert math.isclose(tier1_confidence_dict["Meat"], 0.64, rel_tol=0.01)
    assert tier1_confidence_dict["Vegetables"] is None


@mock_api()
def test_get_tier_confidence_unstreamable():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(INPUT_DATASET_ID)
    df_input = dataframe.from_dataset(dataset)
    dataset.upsert_records(
        df_input.head(1).to_dict(orient="records"), primary_key_name=dataset.key_attribute_names[0]
    )
    project = client.projects.by_resource_id(CATEGORIZATION_PROJECT_ID)

    with pytest.raises(RuntimeError):
        metrics.get_tier_confidence(project)

    # revert to the original state
    all_ops = categorization.jobs.run(project)

    for op in all_ops:
        assert op.succeeded()

    leaf_confidence_dict = metrics.get_tier_confidence(project, tier=-1)

    assert len(leaf_confidence_dict) == 6
    assert math.isclose(leaf_confidence_dict["Dairy|Cheese"], 0.77, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Dairy|Milk"], 0.92, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Beef"], 0.81, rel_tol=0.01)
    assert math.isclose(leaf_confidence_dict["Meat|Chicken|bone-in"], 0.53, rel_tol=0.01)
    assert leaf_confidence_dict["Meat|Chicken|boneless"] is None
    assert leaf_confidence_dict["Vegetables"] is None
