"""Tests for workflow example scripts"""
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api

from examples.scripts.workflow import run_multiple_projects
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    path_to_file=get_toolbox_root_dir() / "examples/resources/conf/project.config.yaml"
)


@mock_api()
def test_run_multiple_projects():
    run_multiple_projects.LOGGER = utils.logger.create(__name__)

    all_ops = run_multiple_projects.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        project_ids=[
            CONFIG["projects"]["my_schema_mapping_project"],
            CONFIG["projects"]["my_categorization_project"],
            CONFIG["projects"]["my_mastering_project"],
            CONFIG["projects"]["my_golden_records_project"],
        ],
    )

    # Check that all operations run completed successfully
    for op in all_ops:
        assert op.succeeded()

    # Check that the number of operations run is exactly what we expect
    assert len(all_ops) == (1 + 2 + 6 + 2)

    # Check that the descriptions of the operations run, match the tasks we wanted to complete
    tamr_client = utils.client.create(**CONFIG["my_tamr_instance"])

    schema_mapping_project = tamr_client.projects.by_resource_id(
        CONFIG["projects"]["my_schema_mapping_project"]
    )
    assert (
        f"Materialize views [{schema_mapping_project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )

    categorization_project = tamr_client.projects.by_resource_id(
        CONFIG["projects"]["my_categorization_project"]
    )
    assert (
        f"Materialize views [{categorization_project.unified_dataset().name}] to Elastic"
        == all_ops[1].description
    )
    assert "Predict record categorizations" == all_ops[2].description

    mastering_project = tamr_client.projects.by_resource_id(
        CONFIG["projects"]["my_mastering_project"]
    )
    assert (
        f"Materialize views [{mastering_project.unified_dataset().name}] to Elastic"
        == all_ops[3].description
    )
    assert "Update Pairs" == all_ops[4].description
    assert "Predict Pairs" == all_ops[5].description
    assert "Generate High-impact Pairs" == all_ops[6].description
    assert "Clustering" == all_ops[7].description
    assert "Publish clusters" == all_ops[8].description

    # golden_records_project
    assert "Updating Golden Records" == all_ops[9].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[10].description
