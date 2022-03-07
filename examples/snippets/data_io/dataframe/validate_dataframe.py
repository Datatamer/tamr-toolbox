"""Snippet for validating the contents of a pandas.Dataframe followed by ingestion into a
Tamr dataset"""
import tamr_toolbox as tbox
import pandas as pd


def my_custom_function():
    pass


# Read config, make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# create DataFrame.  typically this would be from a CSV file
df = pd.read_csv("my_file.csv", dtype="str")

# perform validation
tbox.data_io.dataframe.validate(
    df,
    require_present_columns=["primary_key", "column_1", "column_2"],
    require_unique_columns=["primary_key"],
    require_nonnull_columns=["primary_key", "column_1"],
    custom_check=[my_custom_function, ["column_1", "column_2"]],
)

# can also have validation return a boolean and a dict of failed columns
result = tbox.data_io.dataframe.validate(
    df,
    raise_error=False,
    require_present_columns=["primary_key", "column_1", "column_2"],
    require_unique_columns=["primary_key"],
    require_nonnull_columns=["primary_key", "column_1"],
    custom_check_columns=[my_custom_function, ["column_1", "column_2"]],
)

# result bool will be True if all tests succeed, then proceed to upsert the records from the
# CSV dataset to Tamr
if result.passed:
    tamr_dataset = tamr.datasets.by_resource_id("my_tamr_dataset_id")
    tamr_dataset.upsert_from_dataframe(df, primary_key_name="primary_key")

# if any checks have failed, get the details
else:
    print(f"Validation failed, failing checks: {result.details}")
