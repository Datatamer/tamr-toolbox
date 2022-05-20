"""Tests for tasks related to upstream projects"""
from tamr_toolbox import utils
from tamr_toolbox.utils import upstream
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_get_upstream_projects():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    downstream_project = client.projects.by_resource_id(
        CONFIG["projects"]["minimal_golden_records"]
    )
    projects = upstream.projects(downstream_project)

    assert len(projects) == 2
    assert (
        str(projects) == "[tamr_unify_client.project.resource.Project(relative_id="
        "'projects/1', name='minimal_mastering', "
        "type='DEDUP'), tamr_unify_client.project.resource.Project"
        "(relative_id='projects/2', name='minimal_golden_records', "
        "type='GOLDEN_RECORDS')]"
    )


@mock_api()
def test_get_upstream_datasets():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    downstream_dataset = client.datasets.by_resource_id(
        CONFIG["datasets"][
            "minimal_categorization_unified_dataset_classifications_average_confidences"
        ]
    )

    datasets = upstream.datasets(downstream_dataset)
    assert len(datasets) == 10
