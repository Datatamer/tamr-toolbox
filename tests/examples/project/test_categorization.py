"""Tests for Tamr Categorization project example scripts"""
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api

from examples.scripts.project.categorization import (
    run_categorization_verbose,
    run_categorization_simple,
)
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    path_to_file=get_toolbox_root_dir() / "examples/resources/conf/project.config.yaml"
)


@mock_api()
def test_run_categorization_simple():
    # Set script's global logger
    run_categorization_simple.LOGGER = utils.logger.create(__name__)

    all_ops = run_categorization_simple.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        categorization_project_id=CONFIG["projects"]["my_categorization_project"],
    )

    # Check that all operations run completed successfully
    for op in all_ops:
        assert op.succeeded()

    # Check that the number of operations run is exactly 2
    assert len(all_ops) == 2

    # Check that the descriptions of the operations run, match the tasks we wanted to complete
    tamr_client = utils.client.create(**CONFIG["my_tamr_instance"])
    project = tamr_client.projects.by_resource_id(CONFIG["projects"]["my_categorization_project"])
    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Predict record categorizations" == all_ops[1].description


@mock_api()
def test_run_categorization_verbose():
    # Set script's global logger
    run_categorization_verbose.LOGGER = utils.logger.create(__name__)

    all_ops = run_categorization_verbose.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        categorization_project_id=CONFIG["projects"]["my_categorization_project"],
    )

    tamr_client = utils.client.create(**CONFIG["my_tamr_instance"])
    project = tamr_client.projects.by_resource_id(CONFIG["projects"]["my_categorization_project"])

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 3

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert (
        f"materialize {project.unified_dataset().name}_classification_model"
        == all_ops[1].description
    )
    assert "Predict record categorizations" == all_ops[2].description
