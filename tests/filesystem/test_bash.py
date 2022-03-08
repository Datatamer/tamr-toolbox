"""Tests for tasks related to managing unix-level operations like file and directory management"""
import pytest
import tempfile
from tamr_toolbox.filesystem import bash
import os
from os import path
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, List


def _make_old_files(
    directory_path: str, num_days_old: int, temporary_directory: Path
) -> Tuple[List, List]:
    """
    Creates a simulated directory structure
    with files having modification times set to specified number of days in the past.
    The files are created in a temporary directory which must be supplied

    Args:
        directory_path: Path within temporary directory to create file structure
        num_days_old: number of days in the past to set the file modification times
        temporary_directory: The temporary directory to create backups within

    Returns:
        A tuple of two lists with the first containing the list of paths to the created directories
          and the second containing the list of paths to the created files
    """
    top_level_directory = temporary_directory / directory_path
    directories = [top_level_directory]
    bash.create_directories(directories)

    files = [
        top_level_directory / f"temp_{num_days_old}_days_1",
        top_level_directory / f"temp_{num_days_old}_days_2",
    ]

    # create a timestamp in the past
    mod_timestamp = (datetime.now() - timedelta(days=num_days_old)).timestamp()
    for file in files:
        with open(file, "w"):
            pass
        os.utime(file, (mod_timestamp, mod_timestamp))
    return directories, files


def test_create_and_remove_directories_with_absolute_path():

    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = (
            Path(tempdir) / "test_create_and_remove_directories_with_absolute_path"
        )
        directories = [
            top_level_directory / "test1",
            top_level_directory / "test2",
            top_level_directory / "test3",
        ]

        for directory in directories:
            assert not path.exists(directory), f"Directory {directory} already exists"

        bash.create_directories(directories)

        for directory in directories:
            assert path.exists(directory), f"Created directory {directory} does not exist"

        bash.remove_directories(directories)

        for directory in directories:
            assert not path.exists(directory), f"Removed directory {directory} still exists"

        assert path.exists(top_level_directory), (
            f"Directory {top_level_directory} does not exist " f"after non-recurisve removal"
        )

        bash.remove_directories([top_level_directory])

        assert not path.exists(
            top_level_directory
        ), f"Directory {top_level_directory} still exists after removal"


def test_create_and_remove_directories_with_relative_path():

    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = (
            Path(tempdir) / "test_create_and_remove_directories_with_relative_path"
        )
        directories = [
            top_level_directory / "test1",
            top_level_directory / "test2",
            top_level_directory / "test3",
        ]

        for directory in directories:
            assert not path.exists(directory), f"Directory {directory} already exists"

        bash.create_directories(directories, require_absolute_path=False)

        for directory in directories:
            assert path.exists(directory), f"Created directory {directory} does not exist"

        bash.remove_directories(directories, require_absolute_path=False)

        for directory in directories:
            assert not path.exists(directory), f"Removed directory {directory} still exists"

        assert path.exists(top_level_directory), (
            f"Directory {top_level_directory} does not exist " f"after non-recurisve removal"
        )

        bash.remove_directories([top_level_directory], require_absolute_path=False)

        assert not path.exists(
            top_level_directory
        ), f"Directory {top_level_directory} still exists after removal"


def test_failure_to_remove_directory_with_relative_path():

    directories = ["test1" "test2" "test3"]
    with pytest.raises(ValueError):
        bash.remove_directories(directories)


def test_failure_to_create_directory_with_relative_path():

    directories = ["test1" "test2" "test3"]
    with pytest.raises(ValueError):
        bash.create_directories(directories)


def test_failure_to_remove_non_empty_directory():

    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = Path(tempdir) / "test_failure_to_remove_non_empty_directory"
        directories = [
            top_level_directory / "test1",
            top_level_directory / "test2",
            top_level_directory / "test3",
        ]

        for directory in directories:
            assert not path.exists(directory), f"Directory {directory} already exists"

        # Setup
        bash.create_directories(directories)

        for directory in directories:
            assert path.exists(directory), f"Created directory {directory} does not exist"

        # The test to remove the top level directory first, which should fail
        with pytest.raises(OSError):
            bash.remove_directories([top_level_directory])

        assert path.exists(
            top_level_directory
        ), f"Directory {top_level_directory} removed even though not empty"

        for directory in directories:
            assert path.exists(
                directory
            ), f"Directory {directory} removed due to top-level directory removal"

        # Clean up
        bash.remove_directories(directories)

        bash.remove_directories([top_level_directory])

        for directory in directories:
            assert not path.exists(directory), f"Removed directory {directory} still exists"

        assert not path.exists(
            top_level_directory
        ), f"Directory {top_level_directory} still exists after removal"


def test_failure_to_create_directory_that_already_exists():
    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = (
            Path(tempdir) / "test_failure_to_create_directory_that_already_exists"
        )
        directories = [
            top_level_directory / "test1",
        ]

        for directory in directories:
            assert not path.exists(directory), f"Directory {directory} already exists"

        # Setup
        bash.create_directories(directories)

        # Test that cannot create again
        with pytest.raises(OSError):
            bash.create_directories(directories, failure_if_exists=True)

        # Clean up
        bash.remove_directories(directories)

        bash.remove_directories([top_level_directory])

        for directory in directories:
            assert not path.exists(directory), f"Removed directory {directory} still exists"

        assert not path.exists(
            top_level_directory
        ), f"Directory {top_level_directory} still exists after removal"


def test_failure_to_remove_directory_that_does_not_exist():
    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = (
            Path(tempdir) / "test_failure_to_remove_directory_that_does_not_exist"
        )
        directories = [
            top_level_directory / "test1",
        ]

        for directory in directories:
            assert not path.exists(directory), f"Directory {directory} already exists"

        with pytest.raises(OSError):
            bash.remove_directories(directories, failure_if_not_exists=True)


def test_delete_old_files():
    with tempfile.TemporaryDirectory() as tempdir:
        base_path = Path(tempdir) / "old"
        file_ages_days = [1, 10, 20]
        for age in file_ages_days:
            _make_old_files(base_path, age, tempdir)

        result = bash.delete_old_files(base_path, num_days_to_keep=30)
        assert len(result) == 0

        result = bash.delete_old_files(base_path, num_days_to_keep=14)
        assert len(result) == 2

        result = bash.delete_old_files(base_path, num_days_to_keep=0)
        assert len(result) == 4


def test_delete_old_files_recursive():
    with tempfile.TemporaryDirectory() as tempdir:
        base_path = Path(tempdir) / "old"
        data_path = Path(tempdir) / "old" / "data"
        _make_old_files(data_path, 10, tempdir)

        result = bash.delete_old_files(base_path, num_days_to_keep=0)
        assert len(result) == 2


def test_delete_old_files_exclude():
    with tempfile.TemporaryDirectory() as tempdir:
        base_path = Path(tempdir) / "old"
        data_path = Path(tempdir) / "old" / "data"
        _make_old_files(data_path, 10, tempdir)

        result = bash.delete_old_files(base_path, num_days_to_keep=0, exclude_paths=[data_path])
        assert len(result) == 0

        result = bash.delete_old_files(base_path, num_days_to_keep=0)
        assert len(result) == 2


def test_delete_old_files_invalid_num_days():
    with tempfile.TemporaryDirectory() as tempdir:
        with pytest.raises(ValueError):
            bash.delete_old_files(Path(tempdir), num_days_to_keep=-1)


def test_delete_old_files_invalid_path():
    with tempfile.TemporaryDirectory() as tempdir:
        with pytest.raises(FileNotFoundError):
            bash.delete_old_files(Path(tempdir) / "fake_path", num_days_to_keep=14)
