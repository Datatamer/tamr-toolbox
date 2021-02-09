"""Each instance of `Client` is configured to connect to one jdbc source via the `jdbc_key`
parameter. The default value of *ingest* is what is used in the simple example.
However, the code below shows how to connect to multiple source databases using a more
complicated config file.
"""
import tamr_toolbox as tbox

# load example config file for multiple databases
my_connect_config = tbox.utils.config.from_yaml(
    "examples/resources/conf/connect_multi_ingest.config.yaml"
)

# stream table A into Tamr using 'oracle' db source
my_oracle_connect = tbox.data_io.df_connect.client.from_config(
    my_connect_config, jdbc_key="oracle"
)
tbox.data_io.df_connect.client.ingest_dataset(
    my_oracle_connect, dataset_name="source_A", query="SELECT * FROM schemaA.tableA"
)

# stream table B into Tamr using postgres db source
my_postgres_connect = tbox.data_io.df_connect.client.from_config(
    my_connect_config, jdbc_key="postgres"
)
tbox.data_io.df_connect.client.ingest_dataset(
    my_postgres_connect, dataset_name="source_B", query="SELECT * FROM schemaB.tableB"
)
