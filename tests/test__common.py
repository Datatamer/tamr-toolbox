"""Tests for common tasks to the testing framework only"""

from tests._common import get_toolbox_root_dir
from pathlib import Path


def test__valid_toolbox_root_dir():
    path = get_toolbox_root_dir()
    assert path.exists()
    assert path.is_absolute()
    # test that we can find this file using the toolbox root directory
    assert path / "tests" / "test__common.py" == Path(__file__)
