"""Tasks related to testing code"""
import logging
import json
import os
import re
from inspect import getfile

from pathlib import Path
from time import sleep
from typing import Callable, List, Optional, Union, Dict
from unittest import mock
from functools import wraps
from requests import Response, ConnectionError

from tamr_unify_client.operation import Operation

from tamr_toolbox import utils


LOGGER = logging.getLogger(__name__)

WINDOWS_RESERVED_CHARACTER_MAP = {
    "<": "lt",
    ">": "gt",
    ":": "colon",
    '"': "dquote",
    "/": "fslash",
    "\\": "bslash",
    "|": "pipe",
    "?": "qmark",
    "*": "asterisk",
}


def _response_to_json(resp: Response, ip_dict: Dict[str, int]) -> str:
    """Converts a Response object into json string readable by the responses mocking library
    Args:
        resp: Response from a Tamr API call
        ip_dict: Mapping of previously encountered IP addresses to their anonymization number
    Returns:
        The response represented as a json string
    """
    if resp.encoding is None:
        resp.encoding = "utf-8"
    resp_log = {
        "method": resp.request.method,
        "url": _anonymize_url(resp.request.url, ip_dict),
        "status": resp.status_code,
        "content_type": resp.headers.get("Content-Type"),
        "body": resp.text,
    }
    return json.dumps(resp_log, ensure_ascii=False)


def _anonymize_url(url: str, ip_dict: Dict[str, int]) -> str:
    """Returns a anonymized url. Updates the dictionary inplace if a new ip is encountered

    Args:
        url: A URL
        ip_dict: Previously encountered IP addresses and an assigned numeric value

    Returns:
        URL with the IP address anonymized
    """
    regex_match = re.match(r"(?i)(^https?://)(.*?)([/:].*$)", url)
    ip = regex_match.group(2)

    try:
        num = ip_dict[ip]
    except KeyError:
        ip_dict[ip] = len(ip_dict.values()) + 1
        num = ip_dict[ip]

    return f"{regex_match.group(1)}ip-{num:05d}{regex_match.group(3)}"


def _collect_operation_calls(
    *, response: Response, poll_interval_seconds: int = 3
) -> List[Response]:
    """If the provided response is an Operation, wait for the operation to complete and
    return responses related to that operation for cases where we are running synchronously.

    Args:
        response: A previous Response generated from the same Tamr client
        poll_interval_seconds: Time interval (in seconds) between subsequent polls

    Returns:
        Responses related to polling the operation

    """
    client = utils.client._from_response(response)
    op = Operation.from_response(client, response)
    LOGGER.info(f"Waiting for operation to complete: {op}")

    request_while_pending = client.get(endpoint=f"/api/versioned/v1/operations/{op.resource_id}")

    while op.state == "PENDING":
        op = op.poll()
        sleep(poll_interval_seconds)
    request_while_running = client.get(endpoint=f"/api/versioned/v1/operations/{op.resource_id}")

    op.wait()
    request_when_complete = client.get(endpoint=f"/api/versioned/v1/operations/{op.resource_id}")

    return [request_while_pending, request_while_running, request_when_complete]


def _log_response(
    *, log_path: Path, ip_dict: Dict[str, int], response: Response, asynchronous: bool = False
) -> None:
    """Appends a response to a file. If the response returned is
     a Tamr Operation, poll the operation until complete and log those responses as well
     unless asynchronous is set to True.

    Args:
        log_path: File to write the response to
        ip_dict: Mapping of previously encountered IP addresses to their anonymization number
        response: The response to log
        asynchoronous: Whether or not to `wait` for Operations called during the running of tests

    """
    LOGGER.info(f"logged request: {response.url}")
    with log_path.open(mode="a", encoding="utf-8") as f:
        all_responses = [response]

        # Poll and wait for operations, if applicable
        is_operation_request = bool(
            re.match(re.compile(".*/api/versioned/v1/operations/.*"), response.url)
        )
        is_get_request = response.request.method == "GET"
        is_not_error = response.ok

        if is_get_request and is_operation_request and is_not_error and not asynchronous:
            wait_resp = _collect_operation_calls(response=response)
            all_responses.extend(wait_resp)

        all_json = [_response_to_json(r, ip_dict) for r in all_responses]
        f.writelines([f"{j}\n" for j in all_json])


def _build_response_log_path(
    *, test_func: Callable, response_logs_dir: Optional[Union[str, Path]], **kwargs,
) -> Path:
    """Returns a file path for API response logs for a given test and test parameters

    Args:
        test_func: The test function
        **kwargs: Arguments to the test function

    Returns:
        File path for the API response logs

    """
    # Convert test arguments and their values to a string, skipping ignored arguments
    test_params = "_".join([f"{k}={v}" for k, v in {**kwargs}.items()])
    # Remove reserved characters from ndjson name
    for char in WINDOWS_RESERVED_CHARACTER_MAP:
        test_params = test_params.replace(char, WINDOWS_RESERVED_CHARACTER_MAP[char])

    if len(test_params) > 0:
        test_params = "__" + test_params

    if response_logs_dir is None:
        # If no directory is provided, create a directory with the name of the test file
        # in a directory called "response_logs" located in the same directory as the test file
        dir_matcher = re.match(r"(.*)(?:\\|/)(.*).py", str(Path(getfile(test_func))))

        response_logs_dir = f"{dir_matcher.group(1)}/response_logs/{dir_matcher.group(2)}"

    return Path(f"{response_logs_dir}/{test_func.__name__}{test_params}.ndjson")


def mock_api(
    *,
    response_logs_dir: Optional[Union[str, Path]] = None,
    enforce_online_test: bool = False,
    asynchronous: bool = False,
) -> Callable:
    """Decorator for `pytest` tests that mocks API requests by reading a file of
    pre-generated responses. Will generate responses file based on a real connection
    if pre-generated responses are not found.

    Args:
        response_logs_dir: Directory to read/write response logs
        enforce_online_test: Whether an online test should be run, even if a response log
            already exists
        asynchronous: Whether or not to `wait` for Operations called during the running of tests

    Returns:
        Decorated function

    """

    def wrap(test_function: Callable):
        @wraps(test_function)
        def wrapped(**kwargs):

            response_log_path = _build_response_log_path(
                test_func=test_function, response_logs_dir=response_logs_dir, **kwargs,
            )

            if response_log_path.exists() and enforce_online_test:
                # Delete the file to enforce an online test
                response_log_path.unlink()

            if response_log_path.exists():
                try:
                    LOGGER.info(f"Running offline test based on file at {response_log_path}")
                    _run_offline_test(
                        response_log_path=response_log_path, test_function=test_function, **kwargs,
                    )
                except ConnectionError as e:
                    msg = (
                        f"A required API call was missing from response logs file for this "
                        f"offline test ({response_log_path}). The response log file must be "
                        f"regenerated. Delete the existing file to automatically regenerate a "
                        f"new one. Caused by: {e}"
                    )
                    LOGGER.error(msg)
                    raise ConnectionError(e)
            else:
                _run_online_test(
                    response_log_path=response_log_path,
                    test_function=test_function,
                    asynchronous=asynchronous,
                    **kwargs,
                )

        return wrapped

    return wrap


# Handle ModuleNotFoundError to allow tamr_toolbox to be used when the optional dependency
# `responses` is not installed
try:
    import responses

    # Stores the original _real_send function of requests
    _BASE_FIND_MATCH = responses.RequestsMock._find_match
    # Stores the original _real_send function of responses
    _BASE_SEND_REAL = responses._real_send

    @responses.activate
    @mock.patch.object(Operation.wait, "__defaults__", (0, None))  # sets operation wait time to 0
    def _run_offline_test(response_log_path: Path, test_function: Callable, **kwargs) -> None:
        """Runs a test function against saved API responses located in a file

        Args:
            response_log_path: Location of saved API responses
            test_function: The function to test
            **kwargs: Keyword arguments for the test function

        """
        with response_log_path.open(encoding="utf-8") as f:
            for line in f:
                response = json.loads(line)
                responses.add(**response)

        ip_lookup = {}

        def _find_anonymized_match(self, request):
            """Allows responses library to match requests for an ip address to match to an
            anonymized ip address
            """
            request.url = _anonymize_url(request.url, ip_lookup)
            return _BASE_FIND_MATCH(self, request)

        with mock.patch("responses.RequestsMock._find_match", new=_find_anonymized_match):
            test_function(**kwargs)

    @responses.activate
    def _run_online_test(
        response_log_path: Path, test_function: Callable, asynchronous: bool = False, **kwargs
    ) -> None:
        """Runs a test function against a Tamr instance and saves the API responses to a file

        Args:
            response_log_path: Location to save API responses
            test_function: The function to test
            asynchoronous: Whether or not to run asynchronously
                (i.e. don't `wait` for operations to finish)
            **kwargs: Keyword arguments for the test function

        """
        LOGGER.info(
            f"Online test running against Tamr instance. "
            f"Creating new file at {response_log_path}. This may take a while ..."
        )
        os.makedirs(response_log_path.parent, exist_ok=True)
        response_log_path.touch()

        # Each time an API call is made, allow it to pass through responses and make a real call
        # Each time a real call is made, log the response in the response file
        responses.add_passthru(re.compile(".*"))

        ip_lookup = {}

        def _send_real_with_log(*args, **kwargs) -> Response:
            """Logs the response from BASE_SEND_REAL

            Args:
                *args: The positional arguments for BASE_SEND_REAL
                **kwargs: The keyword arguments for BASE_SEND_REAL

            Returns:
                The response from the call

            """
            response = _BASE_SEND_REAL(*args, **kwargs)

            # Prevent recursion
            with mock.patch("responses._real_send", new=_BASE_SEND_REAL):
                _log_response(
                    log_path=response_log_path,
                    response=response,
                    ip_dict=ip_lookup,
                    asynchronous=asynchronous,
                )
            return response

        with mock.patch("responses._real_send", new=_send_real_with_log):
            test_function(**kwargs)

        # Setting the passthru above permanently changes state for online testing
        # Reset passthru to default
        responses.mock.passthru_prefixes = ()
        responses._default_mock.passthru_prefixes = ()


except ModuleNotFoundError as err:
    # Ensure exception is due to responses package being missing
    if err.msg != "No module named 'responses'":
        raise err

    def _run_offline_test(*args, **kwargs):
        """Dummy function to raise the appropriate exception if the function is called without the
        necessary package installed
        """
        import responses  # noqa: F401

    def _run_online_test(*args, **kwargs):
        """Dummy function to raise the appropriate exception if the function is called without the
        necessary package installed
        """
        import responses  # noqa: F401
