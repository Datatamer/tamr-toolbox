"""Tests for tasks related to validating pandas.Dataframes"""
import pytest
import pandas as pd

from tamr_toolbox.data_io import dataframe


def _get_test_dataframe():
    """
    Produces a simple dataframe for testing

    Returns: Pandas Dataframe
    """
    vals = [["1", "A", "apple"], ["2", "B", "banana"], ["3", "C", "carrot"]]
    return pd.DataFrame(vals, columns=["primary_key", "letter", "produce"], dtype="str")


def test_count_null():
    df = _get_test_dataframe()
    assert dataframe._count_null(df["produce"]) == 0
    df.loc[0, "produce"] = None
    assert dataframe._count_null(df["produce"]) == 1


def test_profile_dataframe():
    df = _get_test_dataframe()
    df_profile = dataframe.profile(df)
    assert df_profile.shape == (3, 3)
    assert df_profile.loc["primary_key", "RecordCount"] == 3
    assert df_profile.loc["primary_key", "DistinctValueCount"] == 3
    assert df_profile.loc["primary_key", "EmptyValueCount"] == 0

    # introduce an empty value
    df.loc[0, "produce"] = None
    df_profile_null = dataframe.profile(df)
    assert df_profile_null.loc["produce", "EmptyValueCount"] == 1


def test_successful_validation():
    df = _get_test_dataframe()
    result = dataframe.validate(
        df,
        require_present_columns=["primary_key", "letter", "produce"],
        require_unique_columns=["primary_key"],
        require_nonnull_columns=["primary_key", "letter", "produce"],
    )
    assert result.passed
    assert len(result.details) == 0


def test_required_column_failure():
    df = _get_test_dataframe()
    # drop a column
    df = df[["primary_key", "letter"]]
    with pytest.raises(ValueError):
        dataframe.validate(df, require_present_columns=["primary_key", "letter", "produce"])


def test_unique_column_failure():
    df = _get_test_dataframe()
    # introduce a repeated value
    df.loc[1, "primary_key"] = "1"
    with pytest.raises(ValueError):
        dataframe.validate(df, require_unique_columns=["primary_key"])


def test_nonnull_column_failure():
    df = _get_test_dataframe()
    # introduce an empty value
    df.loc[0, "produce"] = None
    with pytest.raises(ValueError):
        dataframe.validate(df, require_nonnull_columns=["produce"])


def test_failure_dict_return():
    df = _get_test_dataframe()
    df = df[["primary_key", "letter"]]
    result = dataframe.validate(df, raise_error=False, require_present_columns=["produce"])
    assert not result.passed
    assert "produce" in result.details["failed_present_columns"]


def test_check_custom():
    # check custom entry for dataframe validation function

    #   checks percentage failed -> to-do: [100%, 50%, 0%] pass df
    #   checking for non values -- (check each value  + transform value)
    # checks passed value

    def check_for_none_values(value):
            if value is None:
                return False
            else:
                return True

    df_full = _get_test_dataframe()
    df_semi = pd.DataFrame({"primary_key": ["1", "A", None],
                            "letter": ["2", None, None],
                            "produce": [None, None, None]})
    df_fail = pd.DataFrame({"primary_key": [None, None, None],
                            "letter": [None, None, None],
                            "produce": [None, None, None]})

    failed = dataframe.validate(df_full, custom_check_columns=[check_for_none_values, ["primary_key"]])

