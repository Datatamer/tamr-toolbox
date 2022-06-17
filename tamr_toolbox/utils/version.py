"""Tasks related to the version of Tamr instances"""
import inspect
import json
import logging
import warnings
from collections.abc import Callable

from packaging.version import parse
from tamr_unify_client import Client

LOGGER = logging.getLogger(__name__)
logging.captureWarnings(True)


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
        Will be deprecated in favour of raise_warn_tamr_version()

    Args:
        client: Tamr client
        compare_version: String representation of Tamr version

    Returns:
        None

    See Also:
        raise_warn_tamr_version
        ensure_tamr_version
    """
    warnings.warn("Use `raise_warn_tamr_version'",
                  DeprecationWarning, stacklevel=2)

    current_version = current(client)
    if _as_float(current_version) < _as_float(compare_version):
        raise NotImplementedError(
            f"This function is not available in Tamr {current_version}. "
            f"Upgrade to Tamr {compare_version} or later to use this function."
        )


def is_tamr_version_equal(tamr_version, exact_version):
    return parse(exact_version) == parse(tamr_version)


def is_tamr_version_atleast(tamr_version, min_version):
    return parse(min_version) <= parse(tamr_version)


def is_tamr_version_between(tamr_version, min_version, max_version):
    return parse(min_version) <= parse(tamr_version) <= parse(str(max_version))


def raise_warn_tamr_version(tamr_version, min_version, max_version=None,
                            exact_version=False, response="error"):
    """Check Tamr version and raise error/warn as appropriate. If exact_version is True, max_version will be ignored"""

    allowed_responses = ["error", "warn"]
    if response not in allowed_responses:
        raise ValueError(f"Response must be one of {allowed_responses}")

    message = None
    if exact_version:
        if not is_tamr_version_equal(tamr_version, min_version):
            message = f"Using Tamr version(s) {tamr_version}, " \
                      f"but must be exactly {min_version}."

    elif max_version is not None:
        if not is_tamr_version_atleast(tamr_version, min_version):
            message = f"Using Tamr version(s) {tamr_version}, " \
                      f"but must be at least {min_version}."

    elif not is_tamr_version_between(tamr_version, min_version, max_version):
        message = f"Using Tamr version(s) {tamr_version}, " \
                   f"but must be between {min_version} and {max_version}."

    if message and (response == "error"):
        raise EnvironmentError(message)
    elif message and (response == "warn"):
        warnings.warn(message)


def _get_tamr_versions_from_function_args(*args, **kwargs):
    all_args = locals()
    args = [arg for arg in all_args["args"]]
    kwargs = list(all_args["kwargs"].values())
    all_args_parsed = args + kwargs
    response = []

    # Return the client if we can get it
    for arg in all_args_parsed:
        if type(arg) is Client:
            response.append(current(arg))
        elif hasattr(arg, "client"):
            if type(arg.client) is Client:
                response.append(current(arg.client))

    return response


def ensure_tamr_version(min_version, max_version="9999", exact_version=False):
    """Pie decorator for Tamr version checking

    Examples
    --------
    @ensure_tamr_version(min_version=2022.002)
    def my_toolbox_function(tamr_dataset, *args, **kwargs)

    Notes
    -------
    The Tamr version is only checked for arguments going into the function,
    and not new instances of Tamr referred to within function code

    See Also
    -------
    utils.version.is_tamr_version_equal
    utils.version.is_tamr_version_between
    """

    def _decorator(func):
        def _inspector(*args, **kwargs):
            for tamr_version in _get_tamr_versions_from_function_args(*args, **kwargs):
                raise_warn_tamr_version(tamr_version, min_version, max_version, exact_version, response="error")
            return func(*args, **kwargs)

        return _inspector

    return _decorator


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
