"""Tasks related to the version of Tamr instances"""
from tamr_unify_client import Client
from typing import Callable
import json
import inspect

import logging

LOGGER = logging.getLogger(__name__)


def current(client: Client) -> str:
    """Gets the version of Tamr for provided client

    Args:
        client: Tamr client

    Returns:
        String representation of Tamr version

    """
    url = "/api/versioned/service/version"
    response = client.get(url).successful()
    return json.loads(response.content)["version"]


def _as_float(version: str) -> float:
    """Converts string Tamr version to an orderable numeric representation

    The numeric representation is designed to allow for the ordering of Tamr versions from oldest
    to newest. The values are not guaranteed to be sequential even if the versions are sequential.
    Formats covered:
        year.release.patch (2020.001.1)
        major.minor.patch (0.43.0)

    Args:
        version: String representation of Tamr version

    Returns:
        Numeric representation of Tamr version

    """
    version_split = version.split(".")

    if len(version_split) != 3:
        raise ValueError(f"Tamr version {version} does not match known patterns.")

    version_split = [float(x) for x in version_split]
    return (version_split[0] * 1000) + version_split[1] + (version_split[2] / 10)


def enforce_after_or_equal(client: Client, *, compare_version: str) -> None:
    """Raises an exception if the version of the Tamr client is before the provided compare version

    Args:
        client: Tamr client
        compare_version: String representation of Tamr version

    Returns:
        None

    """
    current_version = current(client)
    if _as_float(current_version) < _as_float(compare_version):
        raise NotImplementedError(
            f"This function is not available in Tamr {current_version}. "
            f"Upgrade to Tamr {compare_version} or later to use this function."
        )


def _deprecated_warning(func: Callable, *, message: str) -> Callable:
    """Decorator to log a warning message when the passed function is called.
    Intended for warning about deprecated functions.

    Args:
        func: The function to attach the warning message to
        message: The warning message
    Returns:
        The decorated function
    """

    def warning(*args, **kwargs):
        try:
            current_frame = inspect.currentframe()
            previous_frame = current_frame.f_back
            calling_lineno = previous_frame.f_lineno
            calling_func = previous_frame.f_code
            LOGGER.warning(
                f"In {calling_func.co_filename}:{calling_func.co_name}:{calling_lineno} {message}"
            )
        finally:
            # Avoid reference cycle
            # https://docs.python.org/3/library/inspect.html#the-interpreter-stack
            del current_frame
            del previous_frame
        return func(*args, **kwargs)

    return warning
