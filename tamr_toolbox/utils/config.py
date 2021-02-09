"""Tasks related to loading and using configuration files"""
from typing import Union, Optional
from pathlib import Path

import yaml
import logging
import re

import os

from tamr_toolbox.models.data_type import JsonDict

LOGGER = logging.getLogger(__name__)


# Define pattern to be used for parsing environment variables in config files
# This pattern searches for the '$' character at the start of variable name and then retrieves
# all other text in the variable name. For example: '$MY_VAR' matches and returns 'MY_VAR'
ENV_VAR_PATTERN = re.compile(r"^\$(.*)$")


def _yaml_env_variable_constructor(loader, node):
    """Replaces values that match the ENV_VAR_PATTERN with
    the value of the corresponding environment variable"""
    value = loader.construct_scalar(node)
    env_match = ENV_VAR_PATTERN.match(value)
    if env_match:
        value = env_match.groups()[0]
        env_var = os.environ.get(value)

        if env_var is None:
            LOGGER.warning(f"No environment variable for {value} found, using None.")
        return env_var
    return value


def _yaml_env_loader(path_to_file: Optional[Union[str, Path]]) -> JsonDict:
    """Reads a yaml file and creates a dictionary, retrieving environment variables as needed

    Args:
        path_to_file: Path to config yaml file

    Returns:
        All configuration variables in a dictionary
    """

    yaml_loader = yaml.SafeLoader
    yaml_loader.add_implicit_resolver("!env_var", ENV_VAR_PATTERN, None)
    yaml_loader.add_constructor("!env_var", _yaml_env_variable_constructor)

    with open(path_to_file, "r") as config_file:
        configs = yaml.load(config_file.read(), Loader=yaml_loader)
    LOGGER.info(f"Configurations have been loaded from {path_to_file}")
    return configs


def from_yaml(
    path_to_file: Optional[Union[str, Path]],
    *,
    default_path_to_file: Optional[Union[str, Path]] = None,
) -> JsonDict:
    """Reads a yaml file and creates a dictionary. Input values can be retrieved from environment
    variables

    Args:
        path_to_file: Path to config yaml file
        default_path_to_file: Path to use if path_to_file is null or empty
    Returns:
        All configuration variables in a dictionary
    """

    # If path_to_file is not populated, use default_path_to_file instead
    if path_to_file is None or (isinstance(path_to_file, str) and len(path_to_file) == 0):
        if default_path_to_file is None or (
            isinstance(default_path_to_file, str) and len(default_path_to_file) == 0
        ):
            not_populated_error = (
                f"No target path or default path has been specified. At least one must have a "
                f"value not: 'path_to_file'='{path_to_file}', "
                f"'default_path_to_file'='{default_path_to_file}'"
            )
            LOGGER.error(not_populated_error)
            raise ValueError(not_populated_error)
        else:
            LOGGER.info(f"Reading configuration file from default: {default_path_to_file}")
            path_to_file = default_path_to_file

    return _yaml_env_loader(path_to_file)
