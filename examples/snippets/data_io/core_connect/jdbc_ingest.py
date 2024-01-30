"""
The below code is an example that reads a config file, shows how to use it to create a
 `Client` object, and then streams an example table into Tamr. Relies on default values and
 everything being specified by config file.
"""
import tamr_toolbox as tbox

# read in all the configurations
CONFIG = tbox.utils.config.from_yaml(
    "examples/resources/conf/core_connect_jdbc_ingest.config.yaml"
)

# create a tamr_client
tamr_client = tbox.utils.client.create(**CONFIG["my_tamr_instance"])

# read the jdbc configurations
jdbc_connect = CONFIG["core_connect"]["jdbc"]["ingest"]

# ingest data
tbox.data_io.core_connect.jdbc_ingest(
    client=tamr_client,
    jdbc_connect=jdbc_connect,
    dataset_name="dataset_A",
    query="SELECT * FROM TABLE_A LIMIT 10",
)
