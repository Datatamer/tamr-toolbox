"""Tests for tasks related to testing code"""
import json
from base64 import b64encode

import pytest
import requests
from requests import Response, Request

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox import utils
from tamr_toolbox.models.operation_state import OperationState
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock


def test__build_response_log_path():
    with_no_params = utils.testing._build_response_log_path(
        test_func=utils.client.create, response_logs_dir="/home/my/dir/response_logs"
    )
    assert with_no_params == Path("/home/my/dir/response_logs/create.ndjson")

    with_params = utils.testing._build_response_log_path(
        test_func=utils.client.create,
        response_logs_dir="/home/my/dir/response_logs",
        username="admin",
        password="fake",
        host="0.0.0.0",
    )
    assert with_params == Path(
        "/home/my/dir/response_logs/create__username=admin_password=fake_host=0.0.0.0.ndjson"
    )

    path_input_type = utils.testing._build_response_log_path(
        test_func=utils.client.create, response_logs_dir=Path("/home/my/dir/response_logs"),
    )
    assert path_input_type == Path("/home/my/dir/response_logs/create.ndjson")


def test___response_to_json():
    mock_request = Request("a_method", "http://some-ip/some/api/loc")

    mock_response = Response()
    mock_response.request = mock_request
    mock_response.status_code = 404
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response._content = b'{"key": "value"}'

    actual_result = utils.testing._response_to_json(mock_response, {})
    expected_result = {
        "method": "a_method",
        "url": "http://ip-00001/some/api/loc",
        "status": 404,
        "content_type": "application/json",
        "body": '{"key": "value"}',
    }
    assert actual_result == json.dumps(expected_result)


def test___response_to_json_special_character():
    mock_request = Request("a_method", "http://some-ip/some/api/loc")

    mock_response = Response()
    mock_response.request = mock_request
    mock_response.status_code = 404
    mock_response.headers = {"Content-Type": "application/json"}
    mock_response._content = b'{"key": "test\xc3\xa9"}'

    actual_result = utils.testing._response_to_json(mock_response, {})
    expected_result = {
        "method": "a_method",
        "url": "http://ip-00001/some/api/loc",
        "status": 404,
        "content_type": "application/json",
        "body": '{"key": "testé"}',
    }
    assert actual_result == json.dumps(expected_result, ensure_ascii=False)


def test__collect_operation_calls():
    # setup mock client
    mock_client = Client(None)

    # setup mock operations
    base_operation_json = {
        "id": "2",
        "type": "SPARK",
        "description": "Profiling [employees_tiny.csv] attributes.",
        "status": {
            "state": "SUCCEEDED",
            "startTime": "2020-07-16T17:57:54.458Z",
            "endTime": "2020-07-16T17:58:22.836Z",
            "message": "",
        },
        "created": {"username": "admin", "time": "2020-07-16T17:57:28.920Z", "version": "82"},
        "lastModified": {
            "username": "system",
            "time": "2020-07-16T17:58:23.977Z",
            "version": "119",
        },
        "relativeId": "operations/2",
    }

    operation_states = [
        OperationState.SUCCEEDED,
        OperationState.PENDING,
        OperationState.CANCELED,
        OperationState.RUNNING,
        OperationState.FAILED,
    ]
    mocks = {}

    for state in operation_states:
        op_json = base_operation_json.copy()
        op_json["status"]["state"] = state.value
        mock_operation = Operation.from_json(mock_client, op_json)

        mock_response = Response()
        mock_response._content = json.dumps(op_json).encode("utf-8")
        mock_response.status_code = 200

        mocks[state] = {"op": mock_operation, "response": mock_response}

    # test succeeded with many pending
    mock_client.get = MagicMock(
        side_effect=[
            # response while pending
            mocks[OperationState.PENDING]["response"],
            # polling
            mocks[OperationState.PENDING]["response"],
            mocks[OperationState.PENDING]["response"],
            mocks[OperationState.PENDING]["response"],
            mocks[OperationState.PENDING]["response"],
            mocks[OperationState.PENDING]["response"],
            mocks[OperationState.RUNNING]["response"],
            # response while running
            mocks[OperationState.RUNNING]["response"],
            # response while waiting
            mocks[OperationState.SUCCEEDED]["response"],
            # response when complete
            mocks[OperationState.SUCCEEDED]["response"],
        ]
    )

    with patch("tamr_toolbox.utils.client._from_response", return_value=mock_client):
        result_success = utils.testing._collect_operation_calls(
            response=mocks[OperationState.PENDING]["response"], poll_interval_seconds=0
        )

    assert len(result_success) == 3
    for resp in result_success:
        assert resp.json()["id"] == "2"
    assert result_success[0].json()["status"]["state"] == OperationState.PENDING.value
    assert result_success[1].json()["status"]["state"] == OperationState.RUNNING.value
    assert result_success[2].json()["status"]["state"] == OperationState.SUCCEEDED.value

    # test failed quickly
    mock_client.get = MagicMock(
        side_effect=[
            # response while pending
            mocks[OperationState.PENDING]["response"],
            # polling
            mocks[OperationState.FAILED]["response"],
            # response while running
            mocks[OperationState.FAILED]["response"],
            # response while waiting
            mocks[OperationState.FAILED]["response"],
            # response when complete
            mocks[OperationState.FAILED]["response"],
        ]
    )

    with patch("tamr_toolbox.utils.client._from_response", return_value=mock_client):
        result_failed = utils.testing._collect_operation_calls(
            response=mocks[OperationState.PENDING]["response"], poll_interval_seconds=0
        )

    assert len(result_failed) == 3
    for resp in result_failed:
        assert resp.json()["id"] == "2"
    assert result_failed[0].json()["status"]["state"] == OperationState.PENDING.value
    assert result_failed[1].json()["status"]["state"] == OperationState.FAILED.value
    assert result_failed[2].json()["status"]["state"] == OperationState.FAILED.value


def test__run_offline_test():
    manual_response_file_path = (
        Path(__file__).parent.resolve() / "response_logs/test_testing/manual_response_file.ndjson"
    )

    def example_requests(op: str) -> None:
        """A function that makes arbitrary API requests

        Args:
            op: The operation to request as an example
        """
        creds = b64encode("user:pass".encode("utf-8")).decode("utf-8")
        headers = {"Authorization": f"BasicCreds {creds}"}
        url_1 = f"http://99.99.9.999:9100/api/versioned/v1/operations/{op}"
        requests.get(url=url_1, headers=headers)

        url_2 = f"http://99.99.9.999:9100/api/service/health"
        requests.get(url=url_2, headers=headers)

    # By wrapping the function in a Mock, we can assert that it is called
    example_requests = Mock(side_effect=example_requests)

    # When called with the ip in the log file,
    # the function should complete without errors and run our test function exactly once
    utils.testing._run_offline_test(
        response_log_path=manual_response_file_path, test_function=example_requests, op="545",
    )
    example_requests.assert_called_once()

    # When called with a operation value NOT in the file, a connection error is expected
    with pytest.raises(requests.exceptions.ConnectionError):
        utils.testing._run_offline_test(
            response_log_path=manual_response_file_path, test_function=example_requests, op="7",
        )


def test__run_online_test():
    response_log_directory = Path(__file__).parent.resolve() / "response_logs/test_testing"
    output_response_file_path = response_log_directory / "online_test.ndjson"

    def ping_websites() -> None:
        """A function that makes arbitrary API requests"""
        r1 = requests.get(url="http://www.google.com")
        r2 = requests.get(url="https://www.google.com")
        r3 = requests.get(url="http://www.bing.com")
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r3.status_code == 200

    # By wrapping the function in a Mock, we can assert that it is called
    ping_websites = Mock(side_effect=ping_websites)

    utils.testing._run_online_test(
        response_log_path=output_response_file_path, test_function=ping_websites
    )

    ping_websites.assert_called_once()

    with output_response_file_path.open(encoding="utf-8") as f:
        actual_response_lines = [json.loads(line) for line in f]

    # all should be gets with non empty body contents
    for each in actual_response_lines:
        assert each["method"] == "GET"
        assert len(each["body"]) > 0

    assert actual_response_lines[0]["url"] == "http://ip-00001/"
    assert actual_response_lines[1]["url"] == "https://ip-00001/"
    assert actual_response_lines[2]["url"] == "http://ip-00002/"

    # Delete created response log file
    output_response_file_path.unlink()


def test_mock_api_with_present_file():
    response_log_directory = Path(__file__).parent.resolve() / "response_logs/test_testing"
    present_file = response_log_directory / "some_file.ndjson"

    try:
        with patch(
            "tamr_toolbox.utils.testing._build_response_log_path", return_value=present_file
        ):
            with patch("tamr_toolbox.utils.testing._run_online_test") as online_function_1:
                with patch("tamr_toolbox.utils.testing._run_offline_test") as offline_function_1:

                    # When run with a path that does exist, an offline test should occur
                    @utils.testing.mock_api(response_logs_dir=response_log_directory)
                    def example_function_1():
                        """Prints the word 'Hello'"""
                        print("Hello")

                    present_file.touch()
                    example_function_1()
                    online_function_1.assert_not_called()
                    offline_function_1.assert_called_once()

        # When run with a path that does exist with enforce_online_test flag set to True,
        # an online test should occur
        with patch(
            "tamr_toolbox.utils.testing._build_response_log_path", return_value=present_file
        ):
            with patch("tamr_toolbox.utils.testing._run_online_test") as online_function_2:
                with patch("tamr_toolbox.utils.testing._run_offline_test") as offline_function_2:

                    @utils.testing.mock_api(
                        response_logs_dir=response_log_directory, enforce_online_test=True
                    )
                    def example_function_2():
                        """Prints the word 'Hello'"""
                        print("Hello")

                    present_file.touch()
                    example_function_2()
                    online_function_2.assert_called_once()
                    offline_function_2.assert_not_called()
    finally:
        # Clean up by deleting the test empty file
        if present_file.exists():
            present_file.unlink()


def test_mock_api_with_missing_file():
    response_log_directory = Path(__file__).parent.resolve() / "response_logs/test_testing"
    missing_file = response_log_directory / "missing_file.ndjson"

    # When run with a path that does NOT exist, an online test should occur
    with patch("tamr_toolbox.utils.testing._build_response_log_path", return_value=missing_file):
        with patch("tamr_toolbox.utils.testing._run_online_test") as online_function:
            with patch("tamr_toolbox.utils.testing._run_offline_test") as offline_function:

                @utils.testing.mock_api(response_logs_dir=response_log_directory)
                def example_function():
                    """Prints the word 'Hello'"""
                    print("Hello")

                example_function()
                online_function.assert_called_once()
                offline_function.assert_not_called()


def test_mock_api_with_errors():
    response_log_directory = Path(__file__).parent.resolve() / "response_logs/test_testing"
    present_file = response_log_directory / "empty_file.ndjson"
    present_file.touch()

    try:
        # When run with a path that does exist, an offline test should occur
        # If that path has a missing API call, a ConnectionError should be thrown
        with patch(
            "tamr_toolbox.utils.testing._build_response_log_path", return_value=present_file
        ):

            @utils.testing.mock_api(response_logs_dir=response_log_directory)
            def example_request():
                """Attempt a bad API request"""
                requests.get(url="http://99.99.9.999:9100/api/service/health")

            with pytest.raises(requests.exceptions.ConnectionError):
                example_request()
    finally:
        # Clean up by deleting the test empty file
        if present_file.exists():
            present_file.unlink()


def test__anonymize_url():
    ip1 = "0.0.0.1"
    ip2 = "second-ip-name"
    ip3 = "3.3.3.3.3"

    urls = [
        f"http://{ip1}/api/test",
        f"http://{ip2}/health",
        f"http://{ip1}/health",
        f"https://{ip1}:8030/api/test",
        f"https://{ip3}/other",
    ]

    expected = [
        f"http://ip-00001/api/test",
        f"http://ip-00002/health",
        f"http://ip-00001/health",
        f"https://ip-00001:8030/api/test",
        f"https://ip-00003/other",
    ]

    ip_lookup1 = {}
    results1 = [utils.testing._anonymize_url(u, ip_lookup1) for u in urls]
    assert results1 == expected

    # make sure the values reset when using a different input dict
    ip_lookup2 = {}
    results2 = [utils.testing._anonymize_url(u, ip_lookup2) for u in urls]
    assert results2 == expected
