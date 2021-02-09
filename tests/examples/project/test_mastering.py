"""Tests for Tamr Mastering project example scripts"""
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api

from examples.scripts.project.mastering import run_mastering_simple, run_mastering_verbose
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    path_to_file=get_toolbox_root_dir() / "examples/resources/conf/project.config.yaml"
)


@mock_api()
def test_run_mastering_simple():
    run_mastering_simple.LOGGER = utils.logger.create(__name__)

    all_ops = run_mastering_simple.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        mastering_project_id=CONFIG["projects"]["my_mastering_project"],
    )

    # Check that all operations run completed successfully
    for op in all_ops:
        assert op.succeeded()
    # Check that the number of operations run is exactly 5
    assert len(all_ops) == 6

    # Check that the descriptions of the operations run, match the tasks we wanted to complete
    tamr_client = utils.client.create(**CONFIG["my_tamr_instance"])
    project = tamr_client.projects.by_resource_id(CONFIG["projects"]["my_mastering_project"])
    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Update Pairs" == all_ops[1].description
    assert "Predict Pairs" == all_ops[2].description
    assert "Generate High-impact Pairs" == all_ops[3].description
    assert "Clustering" == all_ops[4].description
    assert "Publish clusters" == all_ops[5].description


@mock_api()
def test_run_mastering_verbose():
    run_mastering_verbose.LOGGER = utils.logger.create(__name__)

    all_ops = run_mastering_verbose.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        mastering_project_id=CONFIG["projects"]["my_mastering_project"],
    )
    for op in all_ops:
        assert op.succeeded()
    assert len(all_ops) == 8

    tamr_client = utils.client.create(**CONFIG["my_tamr_instance"])
    project = tamr_client.projects.by_resource_id(CONFIG["projects"]["my_mastering_project"])

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert (
        all_ops[1].description == "Generate Pair Estimates"
        or len(all_ops[1].status["message"]) == 0  # Indicating a 204, already up to date
    )
    assert "Update Pairs" == all_ops[2].description
    assert "Train Mastering Model" == all_ops[3].description
    assert "Predict Pairs" == all_ops[4].description
    assert "Generate High-impact Pairs" == all_ops[5].description
    assert "Clustering" == all_ops[6].description
    assert "Publish clusters" == all_ops[7].description
