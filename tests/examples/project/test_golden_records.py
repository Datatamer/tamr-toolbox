"""Tests for Tamr Golden Records project example scripts"""
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api

from examples.scripts.project.golden_records import (
    run_golden_records_simple,
    run_golden_records_verbose,
)
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    path_to_file=get_toolbox_root_dir() / "examples/resources/conf/project.config.yaml"
)


@mock_api()
def test_run_golden_records_simple():
    run_golden_records_simple.LOGGER = utils.logger.create(__name__)

    all_ops = run_golden_records_simple.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        golden_records_project_id=CONFIG["projects"]["my_golden_records_project"],
    )

    # Check that all operations run completed successfully
    for op in all_ops:
        assert op.succeeded()

    # Check that the number of operations run is exactly 2
    assert len(all_ops) == 3

    # Check that the descriptions of the operations run, match the tasks we wanted to complete
    assert "Updating all profiling information for Golden Records" == all_ops[0].description
    assert "Updating Golden Records" == all_ops[1].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[2].description


@mock_api()
def test_run_golden_records_verbose():
    run_golden_records_verbose.LOGGER = utils.logger.create(__name__)

    all_ops = run_golden_records_verbose.main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        golden_records_project_id=CONFIG["projects"]["my_golden_records_project"],
    )

    # Check that all operations run completed successfully
    for op in all_ops:
        assert op.succeeded()

    # Check that the number of operations run is exactly 2
    assert len(all_ops) == 2

    # Check that the descriptions of the operations run, match the tasks we wanted to complete
    assert "Updating Golden Records" == all_ops[0].description
    assert "Updating published datasets for Golden Records" == all_ops[1].description
