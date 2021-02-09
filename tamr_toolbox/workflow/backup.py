"""Tasks related to backup and restore of Tamr instances"""
import logging
import requests
from tamr_unify_client import Client
from tamr_toolbox.filesystem import bash
from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox import utils
from typing import Generator, Union, Optional, List
from datetime import datetime
import os
import json
from pathlib import Path

LOGGER = logging.getLogger(__name__)


def list_backups(client: Client) -> Generator[JsonDict, None, None]:
    """Lists all backups available to Tamr client. Will list both succeeded and failed backups.

    Args:
        client: A client object
    Returns:
        A generator of json dict objects for the backups available to client."""
    response = client.get("backups")
    for backup in response.json():
        yield backup


def get_backup_by_id(client: Client, backup_id: str) -> JsonDict:
    """Fetches the json object for a given backup ID.

    Args:
        client: A Tamr client object.
        backup_id: The relativeID corresponding to the desired backup.

    Returns:
        Json dict corresponding to the desired backup.

    Raises:
        ValueError: Raised if GET request to Tamr fails
        """
    api_string = f"backups/{backup_id}"
    response = client.get(api_string)

    if not response.ok:
        message = (
            f"Received non-200 response code '{response.status_code}' "
            f"with message '{response.json()['message']}': '{response.json()}'"
        )
        LOGGER.error(message)
        raise ValueError(message)

    return response.json()


def initiate_backup(
    client: Client,
    *,
    poll_interval_seconds: int = 30,
    polling_timeout_seconds: Optional[int] = None,
    connection_retry_timeout_seconds: int = 600,
) -> requests.Response:
    """Runs a backup of Tamr client and waits until it is finished.

    Args:
        client: A Tamr client object
        poll_interval_seconds: Amount of time in between polls of job state.
        polling_timeout_seconds: Amount of time before a timeout error is thrown.
        connection_retry_timeout_seconds: Amount of time before timeout error is thrown during
            connection retry

    Returns:
        Json dict of response from API request."""
    response = client.post("backups")
    if not response.ok:
        message = f"Received non-200 response code '{response.status_code}': {response.json()}"
        LOGGER.error(message)
        raise RuntimeError(message)

    backup_id = response.json()["relativeId"]

    op = utils.client.poll_endpoint(
        client=client,
        api_endpoint=f"backups/{backup_id}",
        poll_interval_seconds=poll_interval_seconds,
        polling_timeout_seconds=polling_timeout_seconds,
        connection_retry_timeout_seconds=connection_retry_timeout_seconds,
    )
    return op


def initiate_restore(
    client: Client,
    backup_id: str,
    *,
    polling_timeout_seconds: Optional[int] = None,
    poll_interval_seconds: int = 30,
    connection_retry_timeout_seconds: int = 600,
) -> requests.Response:
    """Restores the Tamr client to the state of the supplied backup.

    Args:
        client: A Tamr client object
        backup_id: BackupId of the desired backup.
        polling_timeout_seconds: Amount of time before a timeout error is thrown.
        poll_interval_seconds: Amount of time in between polls of job state.
        connection_retry_timeout_seconds: Amount of time before timeout error is thrown during
            connection retry

    Returns:
        Json dict of response from API request.

    Raises:
        ValueError: Raised if the target backup contains errors
        RuntimeError: Raised if the restore fails to start
        """
    backup = get_backup_by_id(client=client, backup_id=backup_id)
    backup_state = backup["state"]
    if not backup_state == "SUCCEEDED":
        value_error_message1 = (
            f"Backup file with ID {backup_id} did not succeed and has status {backup_state}"
        )
        raise ValueError(value_error_message1)

    error_message = backup["errorMessage"]
    if not error_message == "":
        value_error_message2 = (
            f"Backup file with ID {backup_id} contains non-null error message {error_message}"
        )
        LOGGER.error(value_error_message2)
        raise ValueError(value_error_message2)

    response = client.post("instance/restore", data=backup_id)
    if not response.ok:
        runtime_error_message = (
            f"Received non-200 response code '{response.status_code}' : {response.json()}"
        )
        LOGGER.error(runtime_error_message)
        raise RuntimeError(runtime_error_message)

    op = utils.client.poll_endpoint(
        client=client,
        api_endpoint="instance/restore",
        poll_interval_seconds=poll_interval_seconds,
        polling_timeout_seconds=polling_timeout_seconds,
        connection_retry_timeout_seconds=connection_retry_timeout_seconds,
    )

    return op


def validate_backup(
    directory: Union[Path, str], *, backup_datetime_format: str = "%Y-%m-%d_%H-%M-%S-%f"
) -> bool:
    """Validates that a directory is a valid backup. A valid backup has a
    manifest file, a completion file (_SUCCEEDED, _FAILED, or _CANCELED),
    the folder has a valid date format, and the date is prior to the current time

    Args:
        directory: path to backup directory
        backup_datetime_format: String datetime format in backup folder name

    Returns:
        True if directory is a valid backup, otherwise False.

    """
    if not isinstance(directory, Path):
        directory = Path(directory)
    backup_name = directory.stem

    if not os.path.exists(directory / "manifest.json") and not os.path.exists(
        directory / "manifest.yaml"
    ):
        LOGGER.warning(
            f"The following directory '{directory}' is not a valid Tamr Backup. "
            f"Neither manifest.json nor manifest.yaml exist."
        )
        return False
    if not (
        any([os.path.exists(directory / f"_{x}") for x in ["SUCCEEDED", "CANCELED", "FAILED"]])
    ):
        LOGGER.warning(
            f"The following directory '{directory}' is not a valid Tamr Backup "
            f"A SUCCEEDED, CANCELED, or FAILED completion file does not exist."
        )
        return False

    try:
        time = datetime.strptime(backup_name, backup_datetime_format)
    except ValueError:
        LOGGER.warning(
            f"The following directory '{directory}' is not a valid Tamr Backup "
            f"The directory name {backup_name} is not a valid datetime"
        )
        return False

    if datetime.now() > time > datetime(2010, 1, 1, 0, 0, 0, 0):
        return True
    else:
        LOGGER.warning(
            f"The following directory '{directory}' is not a valid "
            f"Tamr Backup the datetime represents an invalid time {time}."
        )
        return False


def delete_backups(
    *,
    backups: List[str],
    backup_directory: Union[Path, str],
    backup_datetime_format: str = "%Y-%m-%d_%H-%M-%S-%f",
) -> List[str]:
    """Deletes backup folders recursively.

    Args:
        backups: list of backups to delete
        backup_directory: Path to backup directory
        backup_datetime_format: String datetime format in backup folder name

    Returns:
        list of deleted backup names
    """
    if not isinstance(backup_directory, Path):
        backup_directory = Path(backup_directory)
    backups_to_delete = sorted(backups, reverse=True)
    LOGGER.info(f"Deleting {len(backups_to_delete)} backup(s): '{backups_to_delete}'")

    deleted_backups = []
    for backup in backups:
        backup_path = os.path.join(backup_directory, backup)
        if validate_backup(Path(backup_path), backup_datetime_format=backup_datetime_format):
            bash.remove_directories([backup_path], allow_recursive_deletes=True)
            deleted_backups.append(backup)
        else:
            LOGGER.error(f"{backup} is not a valid backup to delete.")
    return deleted_backups


def classify_backups(
    backup_directory: Union[Path, str], *, backup_datetime_format: str = "%Y-%m-%d_%H-%M-%S-%f"
) -> JsonDict:
    """Takes stock of successful and failed valid backups in the backup directory.

    Args:
        backup_directory: Path to backup directory
        backup_datetime_format: String datetime format in backup folder name

    Returns:
        JSON dict with the keys "successful" (List of successful backups) and
        "not_successful" (List of failed or cancelled backups)

    Raises:
        ValueError: if target backup file contains an error message
        """
    if not isinstance(backup_directory, Path):
        backup_directory = Path(backup_directory)
    backups = os.listdir(backup_directory)

    LOGGER.info(f"Found '{len(backups)}' entries in the directory {str(backup_directory)}")

    not_succeeded = []
    succeeded = []
    for backup in backups:
        if validate_backup(
            backup_directory / backup, backup_datetime_format=backup_datetime_format
        ):
            backup_path = backup_directory / backup

            if os.path.exists(backup_path / "_SUCCEEDED"):
                succeeded.append(backup)
                with open(backup_path / "_SUCCEEDED") as f:
                    succeeded_file = json.load(f)
                if not succeeded_file["errorMessage"] is None:
                    raise ValueError(
                        f"Successful backup file {backup_path}/_SUCCEEDED contains non-null "
                        f"error message '{succeeded_file['errorMessage']}'"
                    )
            else:
                not_succeeded.append(backup)

    return {"succeeded": succeeded, "not_succeeded": not_succeeded}


def delete_old_backups(
    backup_directory: Union[Path, str],
    *,
    num_successful_backups_to_keep: int,
    num_failed_backups_to_keep: int,
    backup_datetime_format: str = "%Y-%m-%d_%H-%M-%S-%f",
) -> Optional[List[JsonDict]]:
    """Deletes old backups. Keeps the most recent num_successful_backups_to_keep successful backups
        and the most recent num_failed_backups_to_keep failed backups

    Args:
        backup_directory: Path to backup directory
        num_successful_backups_to_keep: Number of successful backups to keep
        num_failed_backups_to_keep: Number of failed or canceled backups to keep
        backup_datetime_format: String datetime format in backup folder name

    Returns:
        A list of deleted backups. Returns None if no backups are deleted.

    Raises:
        ValueError: if the number of backups to keep is less than 0

    """
    if not isinstance(backup_directory, Path):
        backup_directory = Path(backup_directory)
    if num_successful_backups_to_keep < 0:
        value_error_message_1 = (
            f"Argument for num_successful_backups_to_keep must be greater than 0. "
            f"Found {num_successful_backups_to_keep}"
        )
        LOGGER.error(value_error_message_1)
        raise ValueError(value_error_message_1)

    if num_failed_backups_to_keep < 0:
        value_error_message_2 = (
            f"Argument for num_failed_backups_to_keep must be greater than 0. "
            f"Found '{num_failed_backups_to_keep}'"
        )
        LOGGER.error(value_error_message_2)
        raise ValueError(value_error_message_2)

    LOGGER.info(f"Fetching all backups from directory '{backup_directory}'")
    result = classify_backups(backup_directory, backup_datetime_format=backup_datetime_format)
    succeeded = result["succeeded"]
    not_succeeded = result["not_succeeded"]
    if (len(succeeded) < 1) and (len(not_succeeded) < 1):
        LOGGER.info(f"Found zero backups in directory '{backup_directory}'")
        return None
    else:
        deleted_failed_backups = []
        deleted_successful_backups = []
        if len(succeeded) <= num_successful_backups_to_keep:
            LOGGER.info(
                f"Found {len(succeeded)} successful backup. Keeping all successful backups"
            )
        else:
            LOGGER.info(
                f"Removing {len(succeeded) - num_successful_backups_to_keep} successful backups"
            )
            succeeded = sorted(succeeded, reverse=False)
            LOGGER.warning(
                f"backups {succeeded} from directory {backup_directory} "
                f"which is {type(backup_directory)}"
            )
            deleted_successful_backups = delete_backups(
                backups=succeeded[: len(succeeded) - num_successful_backups_to_keep],
                backup_directory=backup_directory,
                backup_datetime_format=backup_datetime_format,
            )
        if len(not_succeeded) <= num_failed_backups_to_keep:
            LOGGER.info(f"Found {len(not_succeeded)} failed backups. Keeping all failed backups")
        else:
            LOGGER.info(
                f"Removing {len(not_succeeded) - num_failed_backups_to_keep} unsuccessful backups"
            )
            not_succeeded = sorted(not_succeeded, reverse=False)
            deleted_failed_backups = delete_backups(
                backups=not_succeeded[: len(not_succeeded) - num_failed_backups_to_keep],
                backup_directory=backup_directory,
                backup_datetime_format=backup_datetime_format,
            )
        return deleted_failed_backups + deleted_successful_backups


def delete_old_spark_event_logs(
    tamr_home_directory: Union[Path, str], *, num_days_to_keep: int = 14
) -> List[str]:
    """
    Deletes sparkEventLogs older than the specified number of days.
    This assumes that Spark is running locally on the same VM as Tamr
    and that the logs are on the local filesystem.

    Args:
        tamr_home_directory: Path to the Tamr home directory
        num_days_to_keep: Number of days for which to keep logs

    Returns:
        A list of deleted sparkEventLogs files

    Raises:
        ValueError: if num_days_to_keep is less than 0
        FileNotFoundError: if sparkEventLogs directory doesn't exist
    """
    spark_event_log_directory = os.path.join(
        tamr_home_directory, "tamr/unify-data/job/sparkEventLogs"
    )
    if not os.path.exists(spark_event_log_directory):
        message = f"directory does not exist: {spark_event_log_directory}"
        LOGGER.error(message)
        raise FileNotFoundError(message)

    return bash.delete_old_files(spark_event_log_directory, num_days_to_keep=num_days_to_keep)
