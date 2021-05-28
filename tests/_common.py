"""Tasks for use in the testing of the Tamr Toolbox"""
from pathlib import Path


def get_toolbox_root_dir() -> Path:
    """Returns the full path to the root of the toolbox project. For use in toolbox testing only

    Returns:
        Path to the root directory for the toolbox project
    """
    return Path(__file__).parent.parent.resolve()
