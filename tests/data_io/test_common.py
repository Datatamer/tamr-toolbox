"""Tests for tasks common to moving data in and out of Tamr """
import pytest
import pandas as pd
from tamr_toolbox.data_io import common
from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir
from io import StringIO
from functools import partial
import json
from math import isnan
from typing import Optional, List, Any


# Raw export of people_tiny_copy
TEST_DATA = """
"id","first_name","nickname","last_name","ssn"
"3","Rob","","Cohen",""
"5","Jennifer","Jenny","Ames","456"
"2","Bobby","","Cohen","123"
"6","Ben","","Brown","999"
"4","Jen","","Ames","456"
"8","Ticker","Tuck","Smith",""
"1","Robert","Rob","Cohen","123"
"9","Frank","","Zappa","553"
"7","Jeff","","Johnson","999"
"""

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


def _dataframe_equals(left_df: pd.DataFrame, right_df: pd.DataFrame) -> bool:
    """
    Compares two dataframes to determine if they are identical

    Args:
        left_df: Dataframe to compare against right_df
        right_df: Other Dataframe to compare against left_df
    Returns: Boolean indicating if the two data frames are equal
    """
    # Earlier versions of pandas require the column order be the same to pass the equality,
    # though we don't care to avoid this, confirm the column set is identical and then force them
    # to have the same order
    left_cols = left_df.columns.tolist()
    right_cols = right_df.columns.tolist()
    assert set(left_cols) == set(right_cols)
    return left_df[left_cols].equals(right_df[left_cols])


@pytest.mark.parametrize(
    "val, delimiter, force, expected",
    [
        ("test", "|", False, "test"),
        (["test1", "test2"], "|", False, "test1|test2"),
        (["test1", "test2"], "||", False, "test1||test2"),
        (3.5, "|", False, 3.5),
        (3, "|", False, 3),
        (None, "|", False, None),
        ([], "|", False, ""),
        ([None], "|", False, ""),
        (["test", None], "|", False, "test"),
        ([3, 5], "|", True, "3|5"),
        ([3.1, 5.2], "|", True, "3.1|5.2"),
    ],
)
def test_flatten_list(val: Optional[Any], delimiter: str, force: bool, expected: str):
    assert common._flatten_list(val, delimiter=delimiter, force=force) == expected


@pytest.mark.parametrize(
    "val, delimiter, force",
    [
        ([3, 5, 10, 20, 91238123], "|", False),
        ([3.1, 5.2, 2131.12312314], "|", False),
        ([3, 5.2], "|", False),
    ],
)
def test_flatten_list_error(val: Any, delimiter: str, force: bool):
    with pytest.raises(TypeError):
        common._flatten_list(val, delimiter=delimiter, force=force)


@pytest.mark.parametrize(
    "input_list, reference_list, expected",
    [([1, 2], [1, 2, 3], True), ([1, 2, 3], [1, 2], False), ([1, 2, 3], [1, 2, 3], True)],
)
def test_check_column_subset(input_list: List, reference_list: List, expected: bool):

    assert expected == common._check_columns_subset(
        input_list=input_list, reference_list=reference_list, raise_error=False
    )
    if not expected:
        with pytest.raises(ValueError):
            common._check_columns_subset(
                input_list=input_list, reference_list=reference_list, raise_error=True
            )
    else:
        common._check_columns_subset(
            input_list=input_list, reference_list=reference_list, raise_error=True
        )


@mock_api()
def test_yield_records_without_flatten():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset_id = CONFIG["datasets"]["people_tiny_copy"]
    dataset = client.datasets.by_resource_id(input_dataset_id)

    # get raw records with out flattening
    records = []
    for record in common._yield_records(dataset):
        records.append(record)

    # put into json, these will all be mutlivalues except for the id field
    test_df = (
        pd.read_json(json.dumps(records), orient="records", dtype="object")
        .replace("", float("nan"))
        .sort_values(by="id")
        .reset_index(drop=True)
    )

    # put the csv in the same format
    compare_to_df = (
        pd.read_csv(StringIO(TEST_DATA), dtype="object")
        .sort_values(by="id")
        .reset_index(drop=True)
    )
    for column in ["first_name", "nickname", "last_name", "ssn"]:
        compare_to_df[column] = compare_to_df[column].map(
            lambda x: [x] if not (isinstance(x, float) and isnan(x)) else [""]
        )

    assert _dataframe_equals(test_df, compare_to_df)


@mock_api()
def test_yield_records_with_flatten_subset():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset_id = CONFIG["datasets"]["people_tiny_copy"]
    dataset = client.datasets.by_resource_id(input_dataset_id)

    # do the reverse of test_yield_records
    # by flattening first
    flatten_func = partial(common._flatten_list, delimiter="|", force=True)

    columns = ["first_name", "ssn"]

    records = []
    for record in common._yield_records(
        dataset, columns=["id"] + columns, flatten_columns=columns, func=flatten_func
    ):
        records.append(record)

    test_df = (
        pd.read_json(json.dumps(records), orient="records", dtype="object")
        .replace("", float("nan"))
        .sort_values(by="id")
        .reset_index(drop=True)
    )

    # then get the csv to compare to without modification
    compare_to_df = (
        pd.read_csv(StringIO(TEST_DATA), dtype="object")
        .sort_values(by="id")
        .reset_index(drop=True)
    )

    assert test_df[columns].equals(compare_to_df[columns])


@mock_api()
def test_yield_records_with_flatten_all():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset_id = CONFIG["datasets"]["people_tiny_copy"]
    dataset = client.datasets.by_resource_id(input_dataset_id)

    # do the reverse of test_yield_records
    # by flattening first
    flatten_func = partial(common._flatten_list, delimiter="|", force=True)

    # don't specify the columns to flatten implies flattening all
    records = []
    for record in common._yield_records(dataset, func=flatten_func):
        records.append(record)

    test_df = (
        pd.read_json(json.dumps(records), orient="records", dtype="object")
        .replace("", float("nan"))
        .sort_values(by="id")
        .reset_index(drop=True)
    )

    # then get the csv to compare to without modification
    compare_to_df = (
        pd.read_csv(StringIO(TEST_DATA), dtype="object")
        .sort_values(by="id")
        .reset_index(drop=True)
    )

    assert _dataframe_equals(test_df, compare_to_df)


@mock_api()
def test_yield_records_with_incorrect_flatten_arguments():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset_id = CONFIG["datasets"]["people_tiny_copy"]
    dataset = client.datasets.by_resource_id(input_dataset_id)

    with pytest.raises(ValueError):
        for _ in common._yield_records(dataset, flatten_columns=["first_name"], func=None):
            continue
