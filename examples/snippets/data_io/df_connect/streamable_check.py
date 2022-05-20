"""Snippet explicitly check that a dataset is streamable when exporting with df_connect"""
import tamr_toolbox as tbox

# read config and connect Tamr client
my_config = tbox.utils.config.from_yaml("examples/resources/conf/connect_data_export.config.yaml")
my_connect = tbox.data_io.df_connect.client.from_config(my_config)
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Get the Dataset of interest
dataset = tamr.datasets.by_resource_id("my_tamr_dataset_id")

# default tries to export dataset without explicit streamable check
tbox.data_io.df_connect.client.export_dataset(
    my_connect, dataset_name=dataset.name, target_table_name="target_table"
)

# if dataset is not streamable, this will raise an error
if not dataset.status().is_streamable:
    raise RuntimeError(f"Dataset {dataset.name} is not streamable.")
tbox.data_io.df_connect.client.export_dataset(
    my_connect, dataset_name=dataset.name, target_table_name="target_table"
)

# if dataset is not streamable, this will allow it to be refreshed
if not dataset.status().is_streamable:
    op = dataset.refresh()
    tbox.utils.operation.enforce_success(op)
tbox.data_io.df_connect.client.export_dataset(
    my_connect, dataset_name=dataset.name, target_table_name="target_table",
)
