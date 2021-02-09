"""
The below code is an example that reads a config file, shows how to use it to create a
 `Client` object, and then streams an example table into Tamr. Relies on default values and
 everything being specified by config file.
"""
import tamr_toolbox as tbox


my_config = tbox.utils.config.from_yaml("examples/resources/conf/connect.config.yaml")
my_connect = tbox.data_io.df_connect.client.from_config(my_config)

# ingest table A to dataset A
tbox.data_io.df_connect.client.ingest_dataset(
    my_connect, dataset_name="dataset_A", query="SELECT * FROM TABLE_A"
)
