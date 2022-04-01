"""Tasks related to connecting to a Tamr instance"""
import logging
import re
from base64 import b64decode

import requests
from json import dumps
from typing import Optional, Union
from time import sleep, time as now

from requests import Response
from tamr_unify_client import Client
from tamr_unify_client.auth import UsernamePasswordAuth

LOGGER = logging.getLogger(__name__)


def health_check(client: Client) -> bool:
    """
    Query the health check API and check if each service is healthy (returns True)

    Args:
        client: the tamr client

    Returns:
        True if all services are healthy, False if unhealthy
    """
    try:
        response = client.get(endpoint="/api/service/health")
        healthy_status = all([value["healthy"] for value in response.json().values()])
        if healthy_status:
            LOGGER.info(f"Client is healthy: {dumps(response.json(), indent=2)}")
        else:
            LOGGER.error(f"Client is unhealthy: {dumps(response.json(), indent=2)}")
        return healthy_status

    except requests.exceptions.ConnectionError as e:
        LOGGER.error(f"Could not connect to {client.host}. Error: {e}")
        return False


def create(
    *,
    username: str,
    password: str,
    host: str,
    port: Optional[Union[str, int]] = 9100,
    protocol: str = "http",
    store_auth_cookie: bool = False,
    enforce_healthy: bool = False,
) -> Client:
    """Creates a Tamr client from the provided configuration values

    Args:
        username: The username to log access Tamr as
        password: the password for the user
        host: The ip address of Tamr
        port: The port of the Tamr UI. Pass a value of `None` to specify an address with no port
        protocol: https or http
        store_auth_cookie: If true will allow Tamr authentication cookie to be stored and reused
        enforce_healthy: If true will enforce a healthy state upon creation

    Returns:
        Tamr client
    """
    full_address = f"{protocol}://{host}:{port}" if port is not None else f"{protocol}://{host}"
    LOGGER.info(f"Creating client as user {username} at {full_address}.")
    client = Client(
        auth=UsernamePasswordAuth(username=username, password=password),
        host=host,
        port=int(port) if port is not None else None,
        protocol=protocol,
        store_auth_cookie=store_auth_cookie,
    )
    if enforce_healthy:
        if not health_check(client):
            LOGGER.error(f"Tamr is not healthy. Check logs and Tamr.")
            raise SystemError("Tamr is not healthy. Check logs and Tamr.")
    return client


def get_with_connection_retry(
    client: Client, api_endpoint: str, *, timeout_seconds: int = 600, sleep_seconds: int = 20
) -> requests.Response:
    """Will handle exceptions when attempting to connect to the Tamr API.
        This is used to handle connection issues when Tamr restarts due to a restore.

    Args:
        client: A Tamr client object
        api_endpoint: Tamr API endpoint
        timeout_seconds: Amount of time before a timeout error is thrown. Default is 600 seconds
        sleep_seconds: Amount of time in between attempts to connect to Tamr.

    Returns:
        A response object from API request."""
    started = now()
    while timeout_seconds is None or now() - started < timeout_seconds:
        try:
            response = client.get(api_endpoint)
            return response
        except requests.exceptions.ConnectionError as e:
            # If we got for example a connection refused exception, try again later
            LOGGER.warning(f"Caught exception in connect {e}")
            sleep(sleep_seconds)
    raise TimeoutError(f"Took longer than {timeout_seconds} seconds to connect to tamr.")


def poll_endpoint(
    client: Client,
    api_endpoint: str,
    *,
    poll_interval_seconds: int = 3,
    polling_timeout_seconds: Optional[int] = None,
    connection_retry_timeout_seconds: int = 600,
) -> requests.Response:
    """Waits until job has a state of Canceled, Succeeded, or Failed.

    Args:
        client: A Tamr client object
        api_endpoint: Tamr API endpoint
        poll_interval_seconds: Amount of time in between polls of job state.
        polling_timeout_seconds: Amount of time before a timeout error is thrown.
        connection_retry_timeout_seconds: Amount of time before timeout error is thrown
            during connection retry.

    Returns:
        A response object from API request.
        """

    started = now()
    op = get_with_connection_retry(
        client=client,
        api_endpoint=api_endpoint,
        timeout_seconds=connection_retry_timeout_seconds,
        sleep_seconds=poll_interval_seconds,
    )
    state = op.json()["state"]
    while polling_timeout_seconds is None or now() - started < polling_timeout_seconds:
        if state in ["PENDING", "RUNNING"]:
            sleep(poll_interval_seconds)
        elif state in ["CANCELED", "SUCCEEDED", "FAILED"]:
            return op
        op = get_with_connection_retry(
            client=client,
            api_endpoint=api_endpoint,
            timeout_seconds=connection_retry_timeout_seconds,
            sleep_seconds=poll_interval_seconds,
        )
        state = op.json()["state"]
    raise TimeoutError(f"Took longer than {polling_timeout_seconds} seconds to connect to tamr.")


def _from_response(response: Response) -> Client:
    """Creates a Tamr Client based on a previous api response

    Args:
        response: The response to base the Client on

    Returns:
        New Tamr Client based on the previous response
    """
    request = response.request
    url_matcher = re.match(r"(https?)://(.*):(\d{4})(.*)", request.url)

    auth_hash_matcher = re.match(r"BasicCreds (.*)", request.headers.get("Authorization"))
    creds_matcher = re.match(r"(.*):(.*)", b64decode(auth_hash_matcher.group(1)).decode("latin1"))

    return create(
        username=creds_matcher.group(1),
        password=creds_matcher.group(2),
        host=url_matcher.group(2),
        port=url_matcher.group(3),
        protocol=url_matcher.group(1),
    )
