"""Snippet for exporting a dataset from Tamr as a pandas.Dataframe"""
import tamr_toolbox as tbox

# Read config, make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# get dataframe from Tamr Dataset
dataset = tamr.datasets.by_resource_id("my_tamr_dataset_id")
# default will stream all rows and not apply any flattening
df = tbox.data_io.dataframe.from_dataset(dataset)
# get with lists flattened to strings and a subset of columns and rows
df1 = tbox.data_io.dataframe.from_dataset(
    dataset, flatten_delimiter="|", columns=["tamr_id", "last_name", "first_name"], nrows=5
)
# if the Tamr dataset is not streamable, pass this option to allow refreshing it
df2 = tbox.data_io.dataframe.from_dataset(dataset, nrows=5, allow_dataset_refresh=True)

# a dataframe can also be flattened after creation
# default will attempt to flatten all columns
df3 = tbox.data_io.dataframe.flatten(df)
# flatten only a subset of columns, and force non-string inner array types to strings
df4 = tbox.data_io.dataframe.flatten(df, delimiter="|", columns=["last_name"], force=True)
