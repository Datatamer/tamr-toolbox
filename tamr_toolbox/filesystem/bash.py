"""Tasks related to managing unix-level operations like file and directory management"""

import logging
import os
import shutil
from datetime import datetime, timedelta

from typing import List, Union
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def create_directories(
    directories: List[Union[Path, str]],
    *,
    require_absolute_path: bool = True,
    failure_if_exists: bool = False,
) -> None:
    """Creates directories recursively from a list of target directories if not already created

    Args:
        directories:  List of directories to create.
        require_absolute_path: Boolean flag indicating whether to require absolute paths.
        failure_if_exists: Boolean flag requiring failure if attempt to create a directory
            that already exists.
    Returns:
        None
    """
    for directory in directories:
        if not isinstance(directory, Path):
            directory = Path(directory)

        if require_absolute_path and not directory.is_absolute():
            raise ValueError(f"Expected absolute path for creation but received {directory}")

        if os.path.exists(directory):
            message = f"Tried creating {directory} but directory already exists"
            if failure_if_exists:
                raise OSError(message)
            LOGGER.warning(message)
        else:
            LOGGER.info(f"Creating directory {directory}")
            os.makedirs(directory)


def remove_directories(
    directories: List[Union[Path, str]],
    *,
    require_absolute_path: bool = True,
    failure_if_not_exists: bool = False,
    allow_recursive_deletes: bool = False,
) -> None:
    """Removes directories from a list of target directories if created.
    The directory must be empty

    Args:
        directories:  List of directories to remove
        require_absolute_path: Boolean flag indicating whether to require absolute paths.
        failure_if_not_exists: Boolean flag requiring failure if attempt to remove a directory
            that does not exist.
        allow_recursive_deletes: Boolean flag indicating whether to allow recursive deletion.
    Returns:
        None
    """
    LOGGER.warning(f"{directories}")
    for directory in directories:
        if not isinstance(directory, Path):
            directory = Path(directory)

        if require_absolute_path and not directory.is_absolute():
            raise ValueError(f"Expected absolute path for removal but received {directory}")

        if not os.path.exists(directory):
            message = f"Tried removing {directory} but directory does not exist"
            if failure_if_not_exists:
                raise OSError(message)
            LOGGER.warning(message)
        else:
            if allow_recursive_deletes:
                LOGGER.info(f"Removing directory {directory} recursively.")
                shutil.rmtree(directory)
            else:
                LOGGER.info(f"Removing directory {directory}")
                os.rmdir(directory)


def delete_old_files(
    data_path: Union[Path, str],
    *,
    num_days_to_keep: int,
    exclude_paths: List[Union[Path, str]] = None,
) -> List[str]:
    """
    Find all files in the directory data_path and its subdirectory which have not been modified for
    a specified number of days and delete them except if their subdirectory is specified in the
    folders to exclude

    Args:
        data_path: path to the root folder from where to search for files
        num_days_to_keep: number of days to keep files for
        exclude_paths: list of path to exclude from the search

    Returns:
        A list of deleted files

    Raises:
        ValueError: if num_days_to_keep is less than 0
        FileNotFoundError: if data_path doesn't exist
    """
    if num_days_to_keep < 0:
        value_error_message = (
            f"Argument for num_days_to_keep must be greater than 0. Found {num_days_to_keep}"
        )
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)
    # convert Path to str, to use with os.walk
    exclude_paths_strings = [str(p) for p in exclude_paths] if exclude_paths is not None else []
    if not os.path.exists(data_path):
        message = f"directory does not exist: {data_path}"
        LOGGER.error(message)
        raise FileNotFoundError(message)

    time_threshold = datetime.now() - timedelta(days=num_days_to_keep)
    files_to_delete = []
    for directory_path, _, files in os.walk(data_path):
        if directory_path not in exclude_paths_strings:
            for file in files:
                file_path = os.path.join(directory_path, file)
                if datetime.fromtimestamp(os.path.getmtime(file_path)) < time_threshold:
                    files_to_delete.append(file_path)

    LOGGER.info(
        f"Found {len(files_to_delete)} older than the retention policy of {num_days_to_keep} days"
    )
    LOGGER.info(f"Removing files older than the retention policy")
    for file in files_to_delete:
        LOGGER.debug(f"Deleting {file}")
        os.remove(file)

    return files_to_delete
