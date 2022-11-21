"""
Export data from Tamr using df-connect. An example where everything is default in config file,
which implies exported data is written back to same database as ingested from.
"""
import tamr_toolbox as tbox


my_config = tbox.utils.config.from_yaml("examples/resources/conf/connect.config.yaml")
my_connect = tbox.data_io.df_connect.client.from_config(my_config)

tbox.data_io.df_connect.client.export_dataset(
    my_connect, dataset_name="example_dataset", target_table_name="example_target_table"
)
