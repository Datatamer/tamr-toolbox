"""Utilities shared by enrichment services."""
import json
import math
from typing import Any, List


class CustomJsonEncoder(json.JSONEncoder):
    """
    A Class to transform type 'set' to type 'list' and NaN to None when saving objects to JSON.
    """

    def default(self, python_object):
        """
        Transform a set into a list if input is a set

        Args:
            python_object: the python object to be saved to a json format

        Returns:
            Default json encoder format of input object or List if input is a Set
        """
        if isinstance(python_object, set):
            return list(python_object)
        if math.isnan(python_object):
            return None
        return json.JSONEncoder.default(self, python_object)


def _yield_chunk(list_to_split: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into a List of List with constant length

    Args:
        list_to_split: List to split into chunks
        chunk_size: number of items to have in each list after splitting

    Returns:
        A List of List
    """

    # For item i in a range that is a length of l,
    for i in range(0, len(list_to_split), chunk_size):
        # Create an index range for l of n items:
        yield list_to_split[i : i + chunk_size]


def create_empty_mapping(path: str) -> str:
    """
    Create an empty mapping on disk.

    Args:
        path: location where empty mapping is created

    Returns:
        A path to the new empty file
    """
    with open(path, "w") as file:
        file.write(json.dumps({}))
    return path
