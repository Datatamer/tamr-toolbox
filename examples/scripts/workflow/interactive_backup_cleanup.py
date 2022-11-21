"""Example script for an interactive command prompt for Tamr backup management"""
from typing import Union
import argparse
from pathlib import Path
import os
from datetime import datetime
import pandas as pd

import tamr_toolbox as tbox


def main(
    *, backup_directory: Union[Path, str], backup_datetime_format: str = "%Y-%m-%d_%H-%M-%S-%f"
) -> None:
    """Provides prompts to delete or keep files/directories in backup_directory
    Args:
        backup_directory: Path to backup directory
        backup_datetime_format: String datetime format in backup folder name

    """
    if not isinstance(backup_directory, Path):
        backup_directory = Path(backup_directory)
    backups = os.listdir(backup_directory)

    backups = sorted(backups, reverse=True)

    backups_to_delete = []
    for backup_name in backups:
        backup_path = backup_directory / backup_name
        if tbox.workflow.backup.validate_backup(
            backup_path, backup_datetime_format=backup_datetime_format
        ):
            backup_time = datetime.strptime(backup_name, backup_datetime_format)
            if os.path.exists(backup_path / "_SUCCEEDED"):
                succeeded_file = pd.read_json(
                    backup_path / "_SUCCEEDED", orient="records", typ="series"
                )
                if succeeded_file["errorMessage"] is None:
                    response = input(
                        f"Backup {backup_name} completed successfully at {backup_time} "
                        f"with no error message. Would you like to delete (Y/N)?"
                    )
                else:
                    response = input(
                        f"Backup {backup_name} completed successfully at {backup_time} "
                        f"with error message {succeeded_file['errorMessage']}. "
                        f"Would you like to delete (Y/N)?"
                    )

            else:
                response = input(
                    f"Backup {backup_name} failed at {backup_time}. "
                    f"Would you like to delete (Y/N)?"
                )
            while response not in ["Y", "y", "N", "n"]:
                response = input("Invalid response please enter 'Y' or 'N'")
            if response in ["Y", "y"]:
                backups_to_delete.append(backup_name)
        else:
            response = input(
                f"{backup_name} is an invalid backup. Would you like to delete (Y/N)?"
            )
            while response not in ["Y", "y", "N", "n"]:
                response = input("Invalid response please enter 'Y' or 'N'")
            if response in ["Y", "y"]:
                tbox.filesystem.bash.remove_directories(
                    list(backup_path), allow_recursive_deletes=True
                )
    tbox.workflow.backup.delete_backups(
        backups=backups_to_delete,
        backup_directory=backup_directory,
        backup_datetime_format=backup_datetime_format,
    )


if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup_directory", help="path to a directory to cleanup", required=True)
    parser.add_argument(
        "--backup_datetime_format",
        help="String datetime format in backup folder name",
        required=False,
    )
    args = parser.parse_args()

    # Run the main function
    main(
        backup_directory=args.backup_directory, backup_datetime_format=args.backup_datetime_format
    )
