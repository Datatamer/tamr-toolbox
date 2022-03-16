"""Tests for tasks related to backup and restore of Tamr instances"""
import pytest
from os import path
import json
from pathlib import Path
import tempfile
from typing import Tuple, List

from tamr_toolbox.workflow import backup
from tamr_toolbox import utils
from tamr_toolbox.filesystem import bash

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir
from tests.filesystem.test_bash import _make_old_files

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


def _make_backup(
    backup_name: str,
    completion_status: str,
    temporary_directory: Path,
    manifest_extension: str = "json",
) -> Tuple[List, List]:
    """
    Creates a simulated backup with the primary files in place necessary for identification.
    The files are created in a temporary directory that must be supplied

    Args:
        backup_name: The name of the backup to be used as the backup path
        completion_status: The status of the backup to put in the simulated manifest file
        temporary_directory: The temporary directory to create backups within
        manifest_extension: Either "json" or "yaml"
    Returns:
        A tuple of two lists with the first containing the list of paths to the created directories
          and the second containing the list of paths to the created files
    """
    top_level_directory = temporary_directory / backup_name
    directories = [
        top_level_directory / "config",
        top_level_directory / "database",
        top_level_directory / "elasticsearch",
        top_level_directory / "files",
        top_level_directory / "hbase" / "archive" / "data" / "tamr",
        top_level_directory / "files" / "dataset",
        top_level_directory / "files" / "dedup",
        top_level_directory / "files" / "job",
        top_level_directory / "files" / "procurify",
    ]
    bash.create_directories(directories)

    files = [
        top_level_directory / f"manifest.{manifest_extension}",
        top_level_directory / "config" / "config.json",
        top_level_directory / "database" / "pg_dump.out",
        top_level_directory / completion_status,
    ]

    data = {"finished": backup_name, "errorMessage": None}
    for file in files:
        if file == (top_level_directory / "_SUCCEEDED"):
            with open(file, "w") as f:
                json.dump(data, f)
        else:
            with open(file, "w"):
                pass
    return directories, files


@mock_api()
def test_list_backup():
    client = utils.client.create(**CONFIG["toolbox_backup_test_instance"])
    backups = backup.list_backups(client)

    for found_backup in backups:
        assert found_backup["state"] in ("SUCCEEDED", "CANCELED", "FAILED")


@mock_api()
def test_get_backup():
    backup_id = "2020-05-06_19-33-03-982"
    client = utils.client.create(**CONFIG["toolbox_backup_test_instance"])
    op = backup.get_backup_by_id(client, backup_id)

    assert op["state"] == "SUCCEEDED"

    assert op["errorMessage"] == ""


@mock_api()
def test_backup():
    client = utils.client.create(**CONFIG["toolbox_backup_test_instance"])
    op = backup.initiate_backup(client, poll_interval_seconds=0)

    assert op.json()["state"] == "SUCCEEDED"

    assert op.json()["errorMessage"] == ""


@mock_api()
def test_restore():
    client = utils.client.create(**CONFIG["toolbox_backup_test_instance"])

    op = backup.initiate_backup(client, poll_interval_seconds=0)
    backup_id = op.json()["relativeId"]

    assert op.json()["state"] == "SUCCEEDED"

    op = backup.initiate_restore(client, backup_id, poll_interval_seconds=0)

    assert op.json()["state"] == "SUCCEEDED"

    assert op.json()["errorMessage"] == ""


@mock_api()
def test_bad_backup_id():
    backup_id = "2020-04-01_19-54-38-"
    client = utils.client.create(**CONFIG["toolbox_backup_test_instance"])

    with pytest.raises(ValueError):
        backup.initiate_restore(client, backup_id)


@pytest.mark.parametrize(
    "backups,completion",
    [
        (["2020-07-04_13-33-42-871", "2020-07-04_14-33-42-871"], "_SUCCEEDED"),
        (["2020-07-05_13-33-42-871", "2020-07-06_14-33-42-871"], "_CANCELED"),
        (["2020-07-06_13-33-42-871", "2020-07-05_14-33-42-871"], "_FAILED"),
    ],
)
def test_validate_and_remove_backup(backups: List[str], completion: str):
    all_directories = []
    all_files = []
    with tempfile.TemporaryDirectory() as tempdir:
        for backup_name in backups:
            directories, files = _make_backup(backup_name, completion, Path(tempdir))
            all_directories.extend(directories)
            all_files.extend(files)
            valid = backup.validate_backup(Path(tempdir) / backup_name)

            assert valid

        backup.delete_backups(backups=backups, backup_directory=Path(tempdir))

        for directory in all_directories:
            assert not path.exists(directory), f"Removed directory {directory} still exists"

        for file in all_files:
            assert not path.exists(file), f"Removed file {file} still exists"


def test_invalid_date():
    backup_name = "2020-07_12_04_489"
    with tempfile.TemporaryDirectory() as tempdir:
        _make_backup(backup_name, "_SUCCEEDED", Path(tempdir))
        valid = backup.validate_backup(Path(tempdir) / backup_name)

        assert not valid

        backups = [Path(tempdir) / backup_name]
        bash.remove_directories(backups, allow_recursive_deletes=True)


def test_missing_files():
    with tempfile.TemporaryDirectory() as tempdir:
        top_level_directory = Path(tempdir) / "2020-07-04_12_04_489"
        directories = [
            top_level_directory / "config",
            top_level_directory / "database",
            top_level_directory / "elasticsearch",
            top_level_directory / "files",
            top_level_directory / "hbase" / "archive" / "data" / "tamr",
            top_level_directory / "files" / "dataset",
            top_level_directory / "files" / "dedup",
            top_level_directory / "files" / "job",
            top_level_directory / "files" / "procurify",
        ]
        bash.create_directories(directories)

        files = [
            top_level_directory / "config" / "config.json",
            top_level_directory / "database" / "pg_dump.out",
            top_level_directory / "_SUCCEEDED",
        ]

        for file in files:
            with open(file, "w"):
                pass

        valid = backup.validate_backup(Path(top_level_directory))

        assert not valid

        backups = [top_level_directory]
        bash.remove_directories(backups, allow_recursive_deletes=True)


def test_manifest_yaml():
    backup_name = "2020-12-16_19-33-03-982"
    with tempfile.TemporaryDirectory() as tempdir:
        _make_backup(backup_name, "_SUCCEEDED", Path(tempdir), manifest_extension="yaml")
        valid = backup.validate_backup(Path(tempdir) / backup_name)

        assert valid

        backups = [Path(tempdir) / backup_name]
        bash.remove_directories(backups, allow_recursive_deletes=True)


def test_manifest_invalid_extension():
    backup_name = "2020-12-16_19-33-03-982"
    with tempfile.TemporaryDirectory() as tempdir:
        _make_backup(backup_name, "_SUCCEEDED", Path(tempdir), manifest_extension="txt")
        valid = backup.validate_backup(Path(tempdir) / backup_name)

        assert not valid

        backups = [Path(tempdir) / backup_name]
        bash.remove_directories(backups, allow_recursive_deletes=True)


def test_keeping_backups():
    completed_backups = [
        "2020-07-04_13-33-42-871",
        "2020-07-04_14-33-42-871",
        "2020-07-04_15-33-42-871",
        "2020-07-04_16-33-42-871",
        "2020-07-04_17-33-42-871",
    ]
    failed_backups = [
        "2020-07-05_13-33-42-871",
        "2020-07-05_14-33-42-871",
        "2020-07-05_15-33-42-871",
    ]
    completed_backups_keep = [
        "2020-07-04_15-33-42-871",
        "2020-07-04_16-33-42-871",
        "2020-07-04_17-33-42-871",
    ]
    failed_backups_keep = ["2020-07-05_15-33-42-871"]
    with tempfile.TemporaryDirectory() as tempdir:
        for backup_name in completed_backups:
            _make_backup(backup_name, "_SUCCEEDED", Path(tempdir))
            valid = backup.validate_backup(Path(tempdir) / backup_name)

            assert valid

        for backup_name in failed_backups:
            _make_backup(backup_name, "_FAILED", Path(tempdir))
            valid = backup.validate_backup(Path(tempdir) / backup_name)

            assert valid

        result = backup.classify_backups(Path(tempdir))
        assert len(result["succeeded"]) == 5
        assert len(result["not_succeeded"]) == 3

        deleted_backups = backup.delete_old_backups(
            Path(tempdir), num_successful_backups_to_keep=3, num_failed_backups_to_keep=1
        )
        assert len(deleted_backups) == 4

        result = backup.classify_backups(Path(tempdir))
        assert len(result["succeeded"]) == 3
        assert len(result["not_succeeded"]) == 1

        all_backups = completed_backups_keep + failed_backups_keep
        backup.delete_backups(backups=all_backups, backup_directory=Path(tempdir))


def test_keeping_more_failed_backups_than_present():
    failed_backups = [
        "2020-07-05_13-33-42-871",
        "2020-07-05_14-33-42-871",
        "2020-07-05_15-33-42-871",
    ]
    with tempfile.TemporaryDirectory() as tempdir:
        for backup_name in failed_backups:
            _make_backup(backup_name, "_FAILED", Path(tempdir))
            valid = backup.validate_backup(Path(tempdir) / backup_name)

            assert valid

        result = backup.classify_backups(Path(tempdir))
        assert len(result["not_succeeded"]) == 3

        deleted_backups = backup.delete_old_backups(
            Path(tempdir), num_successful_backups_to_keep=3, num_failed_backups_to_keep=5
        )
        assert len(deleted_backups) == 0

        result = backup.classify_backups(Path(tempdir))
        assert len(result["not_succeeded"]) == 3

        backup.delete_backups(backups=failed_backups, backup_directory=Path(tempdir))


def test_delete_spark_event_logs():
    file_ages_days = [1, 10, 20]
    log_dir = "tamr/unify-data/job/sparkEventLogs"
    with tempfile.TemporaryDirectory() as tempdir:
        for age in file_ages_days:
            _make_old_files(log_dir, age, Path(tempdir))

        result = backup.delete_old_spark_event_logs(Path(tempdir), num_days_to_keep=30)
        assert len(result) == 0

        result = backup.delete_old_spark_event_logs(Path(tempdir), num_days_to_keep=14)
        assert len(result) == 2

        result = backup.delete_old_spark_event_logs(Path(tempdir), num_days_to_keep=0)
        assert len(result) == 4


def test_delete_spark_event_logs_invalid_num_days():
    with tempfile.TemporaryDirectory() as tempdir:
        # Make sure filepath is present
        log_dir = "tamr/unify-data/job/sparkEventLogs"
        _make_old_files(log_dir, 0, Path(tempdir))
        with pytest.raises(ValueError):
            backup.delete_old_spark_event_logs(Path(tempdir), num_days_to_keep=-1)


def test_delete_spark_event_logs_invalid_path():
    with tempfile.TemporaryDirectory() as tempdir:
        with pytest.raises(FileNotFoundError):
            backup.delete_old_spark_event_logs(Path(tempdir) / "fake_path")
