"""Tasks related to the version of Tamr instances"""
import inspect
import json
import logging
import warnings
from collections.abc import Callable
from typing import List, Optional

from packaging.version import parse
from tamr_unify_client import Client

LOGGER = logging.getLogger(__name__)
logging.captureWarnings(True)


def current(client: Client) -> str:
    """
    Gets the version of Tamr for provided client

    Args:
        client: Tamr client

    Returns:
        String representation of Tamr version
    """
    url = "/api/versioned/service/version"
    response = client.get(url).successful()
    return json.loads(response.content)["version"]


def _as_float(version: str) -> float:
    """
    Converts string Tamr version to an orderable numeric representation

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
    warnings.warn("Use `packaging.version.parse() instead.'", DeprecationWarning)
    version_split = version.split(".")

    if len(version_split) != 3:
        raise ValueError(f"Tamr version {version} does not match known patterns.")

    version_split = [float(x) for x in version_split]
    return (version_split[0] * 1000) + version_split[1] + (version_split[2] / 10)


def _get_tamr_versions_from_function_args(*args, **kwargs) -> List[str]:
    """
    Gets the Tamr version of any/all relevant inputs

    Args:
        *args: Any argument that may/may not be linkable to a versioned Tamr client
        **kwargs: Any argument that may/may not be linkable to a versioned Tamr client

    Returns:
        List of all Tamr versions inputted to the function

    """
    all_args = locals()
    args = [arg for arg in all_args["args"]]
    kwargs = list(all_args["kwargs"].values())
    all_args_parsed = args + kwargs
    response = []

    # Return the client version (if we can find it)
    for arg in all_args_parsed:
        if type(arg) is Client:
            response.append(current(arg))
        elif hasattr(arg, "client"):
            if type(arg.client) is Client:
                response.append(current(arg.client))

    return response


def is_version_condition_met(
    *,
    tamr_version: str,
    min_version: str,
    max_version: Optional[str] = None,
    exact_version: bool = False,
    raise_error: bool = False,
) -> bool:
    """
    Check if Tamr version is valid.

    Args:
        tamr_version:
            The version of Tamr being considered
        min_version:
            The earliest version of Tamr
        max_version:
            The latest version of Tamr.
            Default None, in which case no max version is tested for.
        exact_version:
            Compare against only one release of Tamr. Default is False
        raise_error:
            If True, raise an error if the version condition is not met. Default is False.

    Raises:
        ValueError: if `min_version` is greater than `max_version`
        EnvironmentError: if `raise_error` is True, and the condition is not met

    Notes:
        Patch versions (major.minor.patch) are excluded from the comparison
        If exact_version is True, max_version will be ignored

    See Also:
        utils.version.func_requires_tamr_version
    """

    error_str = None

    tamr_version_sub = parse(tamr_version).release[:2]
    min_version_sub = parse(min_version).release[:2]

    if exact_version:
        if not min_version_sub == tamr_version_sub:
            error_str = f"must be exactly {min_version}."

    elif max_version:
        max_version_sub = parse(max_version).release[:2]
        if min_version_sub > max_version_sub:
            raise ValueError("min_version must be smaller than max_version")

        if not min_version_sub <= tamr_version_sub <= max_version_sub:
            error_str = f"must be between {min_version} and {max_version}."

    elif not min_version_sub <= tamr_version_sub:
        error_str = f"must be at least {min_version}."

    if error_str and raise_error:
        raise EnvironmentError(f"Using Tamr version(s) {tamr_version}, but " + error_str)
    else:
        return not error_str


def requires_tamr_version(
    min_version: str, max_version: Optional[str] = None, exact_version: bool = False
) -> Callable:
    """
    Pie decorator for Tamr version checking

    Args:
        min_version:
            The earliest version of Tamr that supports the function
        max_version:
            The latest version of Tamr that supports the function.
            Default None, supporting all latest releases of Tamr
        exact_version:
            If True, only support one release of Tamr. Default is False

    Examples:
        >>> @requires_tamr_version(min_version="2021.002")
        >>> def refresh_dataset(tamr_dataset, *args, **kwargs):
        >>>     return tamr_dataset.refresh()

    Raises:
        ValueError: if `min_version` is greater than `max_version`
        EnvironmentError: if `raise_error` is True, and the condition is not met

    Notes:
        This decorator only inspects the Tamr version of arguments going into the
        function, and not new instances of Tamr referred to within functional code

        Patch versions (major.minor.patch) are excluded from the comparison

    See Also:
        utils.version.is_version_condition_met
    """

    def _decorator(func):
        def _inspector(*args, **kwargs):
            for tamr_version in _get_tamr_versions_from_function_args(*args, **kwargs):
                is_version_condition_met(
                    tamr_version=tamr_version,
                    min_version=min_version,
                    max_version=max_version,
                    exact_version=exact_version,
                    raise_error=True,
                )

            return func(*args, **kwargs)

        return _inspector

    return _decorator


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
    warnings.warn("Use `is_version_condition_met'", DeprecationWarning)

    current_version = current(client)
    if _as_float(current_version) < _as_float(compare_version):
        raise NotImplementedError(
            f"This function is not available in Tamr {current_version}. "
            f"Upgrade to Tamr {compare_version} or later to use this function."
        )


def _deprecated_warning(func: Callable, *, message: str) -> Callable:
    """
    Decorator to log a warning message when the passed function is called.
    Intended for warning about deprecated functions.

    Args:
        func: The function to attach the warning message to
        message: The warning message
    Returns:
        The decorated function
    """
    warnings.warn(
        "Use warnings.warn with `DeprecationWarning', ensuring"
        "`logging.captureWarnings' is True",
        DeprecationWarning,
    )

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
