"""Example script for using the mock_api decorator for testing your code"""
import pytest

import tamr_toolbox as tbox

CONFIG = tbox.utils.config.from_yaml("/path/to/my/conf/project.config.yaml")


@tbox.utils.testing.mock_api()
def test_get_latest_operation():
    """
    In this test, we initiate an operation in Tamr and check that the operation we initiated
    is the same operation returned by the tamr_toolbox.utils.operation.get_latest() function

    """
    client = tbox.utils.client.create(**CONFIG["my_tamr_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["my_schema_mapping_project"])
    expected_op = project.unified_dataset().refresh(asynchronous=True)
    actual_op = tbox.utils.operation.get_latest(client)
    assert expected_op.resource_id == actual_op.resource_id


@tbox.utils.testing.mock_api(
    # By default, mock_api will create a directory called "response_logs" alongside your
    # test function and a directory within "response_logs" with the name of your test file.
    # You can set a custom location using the "response_logs_dir" argument
    response_logs_dir="/full/path/to/my_custom_dir",
    # By default, mock_api runs an online test (making real API calls) if no response log file
    # exists and runs an offline test (faking API calls based on a previous run) when a response
    # log file does exist.
    # You can make it so that mock_api always runs an online tests with this flag
    # This is most useful during the development of your tests
    enforce_online_test=True,
)
def test_operation_by_id_details():
    """
    In this test, we initiate an operation in Tamr and check that the operation we initiated
    can be retrieved by its id using the tamr_toolbox.utils.operation.from_resource_id() function
    """
    client = tbox.utils.client.create(**CONFIG["my_tamr_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["my_schema_mapping_project"])
    expected_op = project.unified_dataset().refresh(asynchronous=True)
    actual_op = tbox.utils.operation.from_resource_id(client, job_id=expected_op.resource_id)
    assert expected_op.resource_id == actual_op.resource_id


# This parameterize decorator allows us to run the same test multiple times
# with different arguments for each run. mock_api will generate a separate response file for
# each combination of parameters
@pytest.mark.parametrize(
    "operation_id,expected_description,expected_state",
    [("110", "Update Pairs", "CANCELED"), ("117", "Predict Pairs", "SUCCEEDED")],
)
@tbox.utils.testing.mock_api()
def test_operation_by_id(
    operation_id: str, expected_description: str, expected_state: str,
) -> None:
    """
    In this test, we get operations by their ids and test that their description and status
    match our expectations
    Args:
        operation_id: The id of the target operation
        expected_description: The expected description of the target operation
        expected_state: The expected status of the target operation
    """
    client = tbox.utils.client.create(**CONFIG["my_tamr_instance"])
    actual_op = tbox.utils.operation.from_resource_id(client, job_id=operation_id)
    assert actual_op.description == expected_description
    assert actual_op.state == expected_state
