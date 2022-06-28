"""
Export data from Tamr to Hive using df-connect. Df-connect supports making *external* tables in
Hive see https://cwiki.apache.org/confluence/display/Hive/Managed+vs.+External+Tables
for some discussion on the difference between managed and external tables. The biggest reason to
take this route is performance. It is infeasible to generate a managed table and insert millions
of rows into Hive. Streaming the data to hdfs and then creating an external table that points at
that table is orders of magnitude faster.

Creating an external table consists of three steps

1) stream the data to HDFS as an avro file (note: this assumes that df-connect itself has been
setup to talk the HDFS) into its own directory

2) stream the avro schema file to HDFS in some other directory than the data file
    (steps 1 and 2 are re-orderable)

3) Create a table in Hive telling Hive about the location of the data and the schema file
(Hive needs both where the data is and what the schema is in order to create a table,
this is messy with avro unless you use an avro schema file, see
https://community.cloudera.com/t5/Support-Questions/Is-there-a-way-to-create-Hive-table-based-on-Avro-data/td-p/119473
for some discussion


WARNING: The jdbc execute endpoint is purposefully designed for flexibility of the engineer to
accomplish data pipeline tasks. As such it allows arbitrary SQL to be run. You should take care to
ensure that you neither run any SQL deleterious to the database. Related,
DO NOT RUN ARBITRARY SQL PASSED FROM UNKNOWN SOURCES.
Doing so opens you up to SQL-injection vulnerabilities. https://xkcd.com/327/
"""
from typing import List
from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.data_io.file_system_type import FileSystemType

import tamr_toolbox as tbox

import click


@click.command()
@click.option(
    "--config_file_path",
    default="examples/resources/conf/connect.config.yaml",
    help="the path to a config file containing df-connect connection information.",
)
@click.option(
    "--dataset_name", default="my_dataset", help="the name of the dataset to stream from Tamr."
)
@click.option(
    "--table_name",
    default="my_hive_db.my_hive_table",
    help="the name of the table to create in Hive for this dataset",
)
@click.option("--hdfs_base_path", help="the location in hdfs to use to stream the data/schema")
def main(
    config_file_path: str, dataset_name: str, table_name: str, hdfs_base_path: str
) -> List[JsonDict]:
    """ "Takes a config file (to setup df connect client), locations for both where to put the avro
     file and where to put the avro schema file, along with the hive statement to execute
     for creating a table.

    Args:
        config_file_path: the path to your config file
        dataset_name: the name of the dataset to stream from Tamr to create the table
        table_name: the name of the target table in Hive to create
        hdfs_base_path: the location in hdfs where to put this data. Note that the avro file will
         be in <hdfs_base_path>/data/my_dataset.avro while the schema will be in
         <hdfs_base_path>/schema/my_dataset.avsc

    Returns:
        List of json objects returned by each request that is made
    """

    # setup df_connect client
    my_config = tbox.utils.config.from_yaml(config_file_path)
    my_connect = tbox.data_io.df_connect.client.from_config(my_config)

    # create needed variables
    hdfs_data_path = hdfs_base_path + "/data/"
    hdfs_file_url = hdfs_data_path + f"/{dataset_name}.avro"
    hdfs_schema_url = hdfs_base_path + f"/schema/{dataset_name}.avsc"

    # now generate the create table statement - pay attention to the LOCATION keyword and
    # notice it is the directory of the avro file, not the file itself

    # **** SEE WARNING IN TOP LEVEL COMMENTS ****
    hive_create_table_statement = (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ROW FORMAT SERDE "
        + "'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT "
        + "'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT "
        + "'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' LOCATION "
        + f"'{hdfs_data_path}' TBLPROPERTIES "
        + f"('avro.schema.url'='{hdfs_schema_url}')"
    )

    response_jsons = []
    # export the schema first
    schema_response = tbox.data_io.df_connect.client.export_dataset_avro_schema(
        my_connect, url=hdfs_schema_url, dataset_name=dataset_name, fs_type=FileSystemType.HDFS
    )
    response_jsons.append(schema_response)

    # export the data next
    data_response = tbox.data_io.df_connect.client.export_dataset_as_avro(
        my_connect, url=hdfs_file_url, dataset_name=dataset_name, fs_type=FileSystemType.HDFS
    )
    response_jsons.append(data_response)

    # now create the table via execute endpoint
    execute_response = tbox.data_io.df_connect.client.execute_statement(
        my_connect, statement=hive_create_table_statement
    )
    response_jsons.append(execute_response)

    return response_jsons


if __name__ == "__main__":
    main()
