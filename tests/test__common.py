"""Tests for common tasks to the testing framework only"""
from types import ModuleType

from tests._common import get_toolbox_root_dir
from pathlib import Path
import os
import importlib


def test__valid_toolbox_root_dir():
    path = get_toolbox_root_dir()
    assert path.exists()
    assert path.is_absolute()
    # test that we can find this file using the toolbox root directory
    assert path / "tests" / "test__common.py" == Path(__file__)


def test__import_namespaces():

    def check_subpackage_imports(subpackage: ModuleType, directory_path: Path) -> None:
        """Recursively asserts that all files/directories within a directory path are importable
        from the subpackage

        Args:
            subpackage: The subpackage to check import from
            directory_path: The directory to compare with the subpackage imports
        """
        print(f"Checking all files/directories within {directory_path} are importable")
        # Collect names of packages we expect based on filenames in the directory
        modules_by_files = {
            f.replace(".py", "") for f in os.listdir(directory_path) if not f.startswith("_")
        }
        # Collect names of packages that are importable from the module
        modules_by_namespace_all = set(subpackage.__all__)
        modules_by_namespace_dir = {p for p in subpackage.__dir__() if (not p.startswith("_"))}

        # We use subset here to allow for imports that are not in the direct file system folder
        # such as the project._common files which are imported through specific project types
        assert modules_by_files.issubset(modules_by_namespace_all)
        assert modules_by_files.issubset(modules_by_namespace_dir)

        # For any subpackages that are also directories, perform the same check
        for sub_module_name in modules_by_namespace_dir:
            sub_module_path = directory_path / sub_module_name
            if os.path.isdir(sub_module_path):
                sub_module = importlib.import_module(f"{subpackage.__name__}.{sub_module_name}")
                check_subpackage_imports(sub_module, sub_module_path)

    import tamr_toolbox

    toolbox_dir = get_toolbox_root_dir() / "tamr_toolbox"

    check_subpackage_imports(tamr_toolbox, toolbox_dir)
