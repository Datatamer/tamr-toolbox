from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

from tamr_toolbox.workflow.concurrent import Graph

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


# Note the pipeline under test is chained like so:
#
# minimimal_schema_mapping --> chained_minimal_schema_mapping ---\
#                                                                 ----> chained_minimal_mastering
#                              minimal_categorization -----------/
#

@mock_api()
def test_get_upstream_projects():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    test_project = tamr.projects.by_name(test_project_name)
    upstream_projects = Graph._get_upstream_projects(test_project)
    assert {x.name for x in upstream_projects} == {
        "chained_minimal_schema_mapping",
        "minimal_categorization"
    }


@mock_api()
def test_get_edges():
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    test_project = tamr.projects.by_name(test_project_name)
    edges = Graph._build_edges(test_project, tamr)
    expected_edges = {
        ("chained_minimal_schema_mapping", "chained_minimal_mastering"),
        ("minimal_categorization", "chained_minimal_mastering"),
        ("minimal_schema_mapping", "chained_minimal_schema_mapping")
    }
    assert expected_edges == edges
