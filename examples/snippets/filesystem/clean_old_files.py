"""Example script for cleaning up old files"""
from tamr_toolbox import filesystem


# to recursively delete files with a modification date older than the specified one
filesystem.bash.delete_old_files("/path/to/my/directory", num_days_to_keep=60)

# to exclude some paths underneath the top level directory
filesystem.bash.delete_old_files(
    "/path/to/my/directory", num_days_to_keep=60, exclude_paths=["save", "data/results"]
)
