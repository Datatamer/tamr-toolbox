"""
An example script to demonstrate how to export datasets from Tamr using df_connect
sending multiple datasets to multiple different databases with multiple different
parameters/behaviors
"""
import tamr_toolbox as tbox

# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/connect_multi_export.yaml")

# stream dataset A to Oracle with default export values from config file
my_connect_oracle = tbox.data_io.df_connect.client.from_config(my_config, jdbc_key="oracle")
tbox.data_io.df_connect.client.export_dataset(
    my_connect_oracle, dataset_name="dataset_A", target_table_name="target_A", jdbc_key="oracle"
)

# stream dataset A to Oracle target table B, while truncating before loading and only 1k records
tbox.data_io.df_connect.client.export_dataset(
    my_connect_oracle,
    dataset_name="dataset_A",
    target_table_name="target_B",
    truncate_before_load=True,
    limit_records=1000,
)

# stream dataset A to Postgres, keeping all Tamr-generated columns
my_connect_postgres = tbox.data_io.df_connect.client.from_config(my_config, jdbc_key="postgres")
tbox.data_io.df_connect.client.export_dataset(
    my_connect_postgres,
    dataset_name="dataset_A",
    target_table_name="target_postgres_A",
    columns_exclude_regex="",
)

# stream dataset A to Postgres, flattening arrays into single string with comma separation
tbox.data_io.df_connect.client.export_dataset(
    my_connect_postgres,
    dataset_name="dataset_A",
    target_table_name="target_postgres_B",
    multi_value_delimiter=",",
)
