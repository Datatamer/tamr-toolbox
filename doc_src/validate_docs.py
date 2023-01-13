import logging
import os
from typing import List
from tests._common import get_toolbox_root_dir

LOGGER = logging.getLogger(__name__)


def get_all_python_files() -> List[str]:
    """
    Get all python files located in top-level directories 'examples' and 'tamr_toolbox'

    Returns:
        List of all python files with relative paths to the root directory
    """
    root_dir = get_toolbox_root_dir()
    all_python_files = []
    for full_path, _, files in os.walk(root_dir):
        rel_path = os.path.relpath(full_path, root_dir)
        first_dir = rel_path.split("/", 1)[0]
        if first_dir not in ["examples", "tamr_toolbox"]:
            continue
        if "/." in rel_path:  # skip over any directories starting with '.'
            continue
        for file in files:
            if "__" in file:  # skip over __init__.py files
                continue
            _, ext = os.path.splitext(file)
            if ext == ".py":
                all_python_files.append(os.path.join(rel_path, file))
    return all_python_files


def path_in_full_text(path: str, full_text: str) -> bool:
    """
    Searches full_text for path and returns if present in either format

    Args:
        path: string of path to be searched for
        full_text: text from file to compare against path
    Returns:
        Boolean indicating whether path in any format found in full_text
    """
    path_without_ext, _ = os.path.splitext(path)
    reformatted_path = path_without_ext.replace("/", ".")
    return any(variations in full_text for variations in [path, reformatted_path])


def get_remaining_paths(target_paths: List[str], full_text: str) -> List[str]:
    """
    Get remaining paths that aren't found in full_text

    Args:
        target_paths: list of paths to check if present in full_text
        full_text: text from file to compare against target_paths
    Returns:
        List of remaining paths not found in full_text
    """
    return [path for path in target_paths if not path_in_full_text(path, full_text)]


def crawl_through_doc_src(
    target_paths: List[str], extensions: List[str] = [".md", ".rst"]
) -> List[str]:
    """
    Crawls through doc_src folder and reads all files of extensions specified, checking if paths included

    Args:
        target_paths: list of paths to check if present
        extensions: list of file extensions to check, default .md and .rst
    Returns:
        Remaining paths that weren't found in doc_src files with extensions specified
    """
    root_dir = get_toolbox_root_dir()
    for full_path, _, files in os.walk(root_dir):
        rel_path = os.path.relpath(full_path, root_dir)
        first_dir = rel_path.split("/", 1)[0]
        if first_dir != "doc_src":
            continue
        for file in files:
            _, ext = os.path.splitext(file)
            if ext in extensions:
                f = open(os.path.join(full_path, file), "r")
                full_text = f.read()
                target_paths = get_remaining_paths(target_paths, full_text)
    return target_paths


def main():
    all_python_files = get_all_python_files()
    remaining_python_files = crawl_through_doc_src(target_paths=all_python_files)

    try:
        assert len(remaining_python_files) == 0
    except AssertionError:
        LOGGER.warning("\nASSERTION ERROR: Files Missing from Docs\n")
        [LOGGER.warning(file) for file in remaining_python_files]
        LOGGER.warning("\n")


if __name__ == "__main__":
    main()
