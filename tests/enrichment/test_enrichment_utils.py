"""Tests for enrichment utility functions."""
import json

import pandas as pd
import pytest

from tamr_toolbox.enrichment.enrichment_utils import (
    SetEncoder,
    dataframe_to_tuples,
    join_clean_tuple,
)


def test_default_encoder():
    non_set = [1, 2, 3]
    set_encoder = SetEncoder()
    with pytest.raises(TypeError):
        set_encoder.default(non_set)


def test_join_clean_tuple():
    assert join_clean_tuple(("abc  def", "qwe")) == "ABC  DEF QWE"
    assert join_clean_tuple((None, "")) == ""
    assert join_clean_tuple(("ABC ", "def")) == "ABC DEF"


def test_json_encoder():
    result = json.loads(json.dumps({"x": {1, 3, 2}}, cls=SetEncoder))
    assert isinstance(result["x"], list)
    assert len(result["x"]) == 3
    assert set(result["x"]) == {1, 3, 2}

    result = json.loads(json.dumps({"x": {"y": {1, 3, 2}, "z": [1, 2, 3]}}, cls=SetEncoder))
    assert isinstance(result["x"]["y"], list)
    assert len(result["x"]["y"]) == 3
    assert set(result["x"]["y"]) == {1, 3, 2}
    assert result["x"]["z"] == [1, 2, 3]


def test_dataframe_to_tuples_bad_columns():
    d = pd.DataFrame({"x": [1, 2, 3]})
    with pytest.raises(ValueError, match="Not all columns"):
        dataframe_to_tuples(dataframe=d, columns_to_join=["x", "y"])


def test_dataframe_to_tuples():
    d = pd.DataFrame({"x": [1, 2, 3], "y": ["abc", "def", "ghi"], "z": [None, 2.3, "abc"]})
    result = dataframe_to_tuples(dataframe=d, columns_to_join=["x", "y", "z"])
    assert result == [("1", "abc", None), ("2", "def", "2.3"), ("3", "ghi", "abc")]
