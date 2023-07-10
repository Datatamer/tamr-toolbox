"""Utilities shared by enrichment services."""
import json
import math
from typing import Any, List, Optional, Tuple

import pandas as pd


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


def dataframe_to_tuples(
    dataframe: pd.DataFrame, columns_to_join: List[str]
) -> List[Tuple[Optional[str], ...]]:
    """_summary_

    Args:
        dataframe: a dataframe
        columns_to_join: list of columns to join in the output

    Raises:
        ValueError: if any of the `columns_to_join` do not appear in the dataframe

    Returns:
        list of strig tuples generated from specified columns of the dataframe
    """
    # Check that expexted columns exist
    df_columns = dataframe.columns
    if not all([x in df_columns for x in columns_to_join]):
        raise ValueError(
            f"Not all columns {columns_to_join} exist in input dataframe columns {df_columns}"
        )

    tuples = [
        tuple(x)
        for x in dataframe[columns_to_join]
        .astype(str)
        .replace("nan", None)
        .to_records(index=False)
    ]

    return tuples


def join_clean_tuple(tup: Tuple[Optional[str], ...]) -> str:
    """Join tuple entries, stripping extra leading/trailing whitespace and uppercasing.

    Args:
        tup: tuple of string or None values

    Returns:
        uppercased string made from joining tuple entries.
    """
    return " ".join([x.strip().upper() for x in tup if x])
