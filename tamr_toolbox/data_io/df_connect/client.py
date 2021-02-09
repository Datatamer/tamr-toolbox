"""Tasks related to interacting with the Tamr auxiliary service DF-connect"""
from dataclasses import dataclass
import json
import logging
import requests
from enum import Enum
from tamr_toolbox.data_io.df_connect import jdbc_info
from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.data_io.file_system_type import FileSystemType
from tamr_unify_client.auth import UsernamePasswordAuth
from typing import Dict, List, Union

LOGGER = logging.getLogger(__name__)


@dataclass
class Client:
    """A data class for interacting with df_connect via jdbc.

    Args:
        host: the host where df_connect is running
        port: the port on which df_connect is listening
        protocol: http or https
        base_path: if using nginx-like proxy this is the redirect path
        tamr_username: the tamr account to use
        tamr_password: the password for the tamr account to use
        jbdc_info: configuration information for the jdbc connection
    """

    host: str
    port: str
    protocol: str
    base_path: str
    tamr_username: str
    tamr_password: str
    jdbc_info: jdbc_info.JdbcInfo


def from_config(
    config: JsonDict, config_key: str = "df_connect", jdbc_key: str = "ingest"
) -> Client:
    """Constructs a Client object from a json dictionary.

    Args:
        config: A json dictionary of configuration values
        config_key: block of the config to parse for values. Defaults to 'df_connect'
        jdbc_key: the key used to specify which block of df_connect-->jdbc in configuration to be
            used for picking up database connection information. Defaults to 'ingest'

    Returns:
        A Client object
    """

    # proxy and port redirect are optional
    base_path = config[config_key].get("base_path", "")
    port = config[config_key].get("port", "")

    return Client(
        host=config[config_key]["host"],
        port=port,
        protocol=config[config_key]["protocol"],
        base_path=base_path,
        tamr_username=config[config_key]["tamr_username"],
        tamr_password=config[config_key]["tamr_password"],
        jdbc_info=jdbc_info.from_config(config, config_key=config_key, jdbc_key=jdbc_key),
    )


def create(
    *,
    host: str,
    port: str = "",
    protocol: str,
    base_path="",
    tamr_username: str,
    tamr_password: str,
    jdbc_dict: JsonDict,
) -> Client:
    """
    Simple wrapper for creating an instance of `Client` dataclass object.

    Args:
        host: the host where df_connect is running
        port: the port on which df_connect is listening
        protocol: http or https
        base_path: if using nginx-like proxy this is the redirect path
        tamr_username: the tamr account to use
        tamr_password: the password for the tamr account to use
        jdbc_dict: configuration information for the jdbc connection

    Returns:
        An instance of `tamr_toolbox.data_io.df_connect.Client`
    """
    jdbc_information = jdbc_info.create(**jdbc_dict)
    return Client(
        host=host,
        port=port,
        protocol=protocol,
        base_path=base_path,
        tamr_username=tamr_username,
        tamr_password=tamr_password,
        jdbc_info=jdbc_information,
    )


def _get_url(connect_info: Client, api_path: str) -> str:
    """Constructs and returns url for request to df_connect. Valid for both http and https.

     Args:
         connect_info: An object of type JdbcInfo from which to pull host/port
         api_path: The api endpoint to be called

    Returns:
        url: A string of the request url formatted correctly for that instance of df_connect.
     """
    # handle port:
    if connect_info.port != "":
        port = ":" + connect_info.port
    else:
        port = connect_info.port
    # handle ssl_redirect
    api_path = "/".join([connect_info.base_path, api_path.lstrip("/")])
    api_path = api_path if api_path.startswith("/") else ("/" + api_path)
    if connect_info.protocol == "https":
        url = "https://" + connect_info.host + port + api_path
    else:
        url = "http://" + connect_info.host + port + api_path
    return url


def _get_query_config(jdbc_info: jdbc_info.JdbcInfo,) -> Dict:
    """Packages configuration info into relevant query configuration json (dict) which is used
    for multiple df_connect API calls.

    Args:
        jdbc_info: JdbcInfo object from which to construct the query configuration.

    Returns:
        query_config: A dictionary suitable for usage in all df_connect API calls needing a
            queryConfig parameter
    """
    query_config = {
        "jdbcUrl": jdbc_info.jdbc_url,
        "dbUsername": jdbc_info.db_user,
        "dbPassword": jdbc_info.db_password,
        "fetchSize": jdbc_info.fetch_size,
    }
    return query_config


def _get_export_config(
    multi_value_delimiter: str = "|", limit_records: int = 0, columns_exclude_regex: str = "",
) -> Dict:
    """Packages relevant pieces of JdbcExportInfo object into an exportDataConfig for jdbc export
     in form of json dictionary

    Args:
        multi_value_delimiter: value with which to delimit multivalues. default is |
        limit_records: number of records to stream. default is 0 (export all records)
        columns_exclude_regex: override config file for columnsExcludeRegex, default is empty
            string

    Returns:
        A dictionary suitable for usage in all df_connect API calls around jdbc export
    """

    # build json object
    export_config = {
        "mergedArrayValuesDelimiter": multi_value_delimiter,
        "limitRecords": limit_records,
        "columnsExcludeRegex": columns_exclude_regex,
    }
    return export_config


def _get_avro_url_export_config(url: str, dataset_name: str) -> JsonDict:
    """Generates json needed for using avro(schema) export endpoints.
    Primary key is always set to [] since needing it is an artifact of the df-connect endpoint

    Args:
        url: the url to which the avro(schema) will be written
        dataset_name: the dataset for which the avro(schema) will be written
    """
    return {"url": url, "datasetName": dataset_name, "primaryKey": []}


def get_connect_session(connect_info: Client) -> requests.Session:
    """Returns an authenticated session using Tamr credentials from configuration.
    Raises an exception if df_connect is not installed or running correctly.

    Args:
        connect_info: An instance of a Client object

    Returns:
        An authenticated session

    Raises:
        RuntimeError: if the a connection to df_connect cannot be established
    """
    auth = UsernamePasswordAuth(connect_info.tamr_username, connect_info.tamr_password)
    s = requests.Session()
    s.auth = auth
    s.headers.update({"Content-type": "application/json"})
    s.headers.update({"Accept": "application/json"})

    # test that df_connect is running properly
    url = _get_url(connect_info, "/api/service/health")
    try:
        r = s.get(url)
        r.raise_for_status()
    except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError):
        raise RuntimeError(
            f"Tamr auxiliary service df-df_connect is either not running or not healthy at {url}!"
            f" Did you install it? Df-connect does not come with default Tamr installation."
            f" Check its status and your configuration."
        )
    return s


def ingest_dataset(
    connect_info: Client, *, dataset_name: str, query: str, primary_key=None
) -> JsonDict:
    """
    Ingest a dataset into Tamr via df-df_connect given dataset name, query string, and optional
    list of columns for primary key

    Args:
        dataset_name: Name of dataset
        query: jdbc query to execute in the database and results of which will be loaded into Tamr
        connect_info: A Client object for establishing session and loading jdbc parameters
        primary_key: list of columns to use as primary key. If None then df_connect will generate
            its own primary key

    Returns:
        JSON response from API call

    Raises:
        HTTPError: if the call to ingest the dataset was unsuccessful
    """
    # handle primary key
    if primary_key is None:
        primary_key = []
    else:
        primary_key = primary_key.split(",")

    # establish a df_connect session
    connect_session = get_connect_session(connect_info)

    # ingest data
    api_path = "/api/jdbcIngest/ingest"
    query_config = _get_query_config(connect_info.jdbc_info)
    ingest_data = {
        "query": query,
        "datasetName": dataset_name,
        "primaryKey": primary_key,
        "queryConfig": query_config,
    }
    ingest_url = _get_url(connect_info, api_path)
    LOGGER.info(
        f"Streaming data from {connect_info.jdbc_info.jdbc_url} to "
        f"Tamr with the following query: \n\t{query}"
    )
    r = connect_session.post(ingest_url, data=json.dumps(ingest_data))

    # check if successful and if so return True
    r.raise_for_status()
    return r.json()


def export_dataset(
    connect_info: Client,
    *,
    dataset_name: str,
    target_table_name: str,
    truncate_before_load: bool = False,
    **kwargs,
) -> JsonDict:
    """Export a dataset via jdbc to a target database.

    Args:
        dataset_name: the name of the dataset to export
        target_table_name: the table in the database to update
        truncate_before_load: whether or not to truncate the database table before load
        connect_info: A Client object for establishing session and loading jdbc parameters
        jdbc_key: the key for picking up relevant block for export from config file.
            See examples directory for usage

    Returns:
        JSON response from API call

    Raises:
        HTTPError: if the call to export the dataset was unsuccessful
    """

    # establish a df_connect session
    connect_session = get_connect_session(connect_info)

    # export data
    api_path = "/api/urlExport/jdbc"
    query_config = _get_query_config(connect_info.jdbc_info)
    export_data_config = _get_export_config(**kwargs)
    export_data = {
        "unifyDatasetName": dataset_name,
        "queryConfig": query_config,
        "exportDataConfig": export_data_config,
        "truncateBeforeLoad": truncate_before_load,
        "targetTableName": target_table_name,
    }

    export_url = _get_url(connect_info, api_path)
    LOGGER.info(
        f"Streaming data to {connect_info.jdbc_info.jdbc_url} from this "
        f"Tamr dataset: \n\t{dataset_name}"
    )
    r = connect_session.post(export_url, data=json.dumps(export_data))
    r.raise_for_status()
    return r.json()


def execute_statement(connect_info: Client, statement: str) -> JsonDict:
    """Calls the execute statement endpoint of df-df_connect.

    Args:
        statement: the SQL statement to be executed
        connect_info: A Client object for establishing session and loading jdbc parameters

    Returns:
        JSON response from API call

    Raises:
        HTTPError: if the call to df_connect was unsuccessful
    """

    # establish a df_connect session
    connect_session = get_connect_session(connect_info)

    # parse query config from main configuration
    query_config = _get_query_config(connect_info.jdbc_info)

    # export data
    api_path = "/api/jdbcIngest/execute"
    execute_data = {"queryConfig": query_config, "statement": statement}
    execute_url = _get_url(connect_info, api_path)
    LOGGER.info(
        f"Execute statement {statement} using the following jdbc url {query_config['jdbcUrl']}"
    )
    r = connect_session.post(execute_url, data=json.dumps(execute_data))
    r.raise_for_status()
    return r.json()


def profile_query_results(
    connect_info: Client, *, dataset_name: str, queries: List[str]
) -> JsonDict:
    """
    Profile the contents of JDBC queries via df_connect and write results to a Tamr dataset.
    For example the query "select * from table A" means that all rows from table A will be
    profiled, while "select * from table A where name=="my_name"" will only profile rows meeting
    the name=="my_name" condition.
    The same Tamr dataset can be used for profile results from multiple queries

    Args:
        dataset_name: Name of Tamr dataset for the profiling results
        queries: list of JDBC queries to execute in the database, the results of which will be
            profiled
        connect_info: A Client object for establishing session and loading jdbc parameters

    Returns:
        JSON response from API call

    Raises:
        HTTPError: if the call to profile the dataset was unsuccessful
    """
    # primary key must be passed but isn't used
    primary_key = []

    # establish a df_connect session
    connect_session = get_connect_session(connect_info)

    # run profiling
    api_path = "/api/jdbcIngest/profile"
    query_config = _get_query_config(connect_info.jdbc_info)
    profile_data = {
        "queryTargetList": [
            {"query": query, "datasetName": dataset_name, "primaryKey": primary_key}
            for query in queries
        ],
        "queryConfig": query_config,
    }
    profile_url = _get_url(connect_info, api_path)
    LOGGER.info(
        f"Profiling data from {connect_info.jdbc_info.jdbc_url} to Tamr with the "
        f"following queries: \n\t{queries}"
    )
    r = connect_session.post(profile_url, data=json.dumps(profile_data))

    # check if successful, and if so, return request JSON
    r.raise_for_status()
    return r.json()


def export_dataset_avro_schema(
    connect_info: Client, *, url: str, dataset_name: str, fs_type: Enum
) -> Union[JsonDict, bool]:
    """
    Takes a connect info object and writes the avro schema to specified url for specified
    dataset. By default assumes HDFS but if local_fs is set to true writes to server file system.

    Args:
        connect_info: The connect client to use
        url: the location in the relevant file system to which to write the avro schema
        dataset_name: the name of the dataset
        fs_type: the remote filesystem type. Currently supports 'HDFS' and 'LOCAL'

    Returns:
        json returned by df-connects /urlExport/<hdfs/serverfs>/avroSchema endpoint

    Raises:
        HTTPError: if the call to export the schema was unsuccessful
    """

    if fs_type == FileSystemType.LOCAL:
        api_path = f"/api/urlExport/{fs_type.value}/avroSchema"
    elif fs_type == FileSystemType.HDFS:
        api_path = f"/api/urlExport/{fs_type.value}/avroSchema"
    else:
        error = (
            f"trying to use unsupported type {fs_type}, supported are "
            f"'{FileSystemType.LOCAL.value}' and '{FileSystemType.HDFS.value}'"
        )
        LOGGER.error(error)
        raise ValueError(error)

    url_export_config = _get_avro_url_export_config(url, dataset_name)

    # establish a df_connect session and make API call
    connect_session = get_connect_session(connect_info)
    url = _get_url(connect_info, api_path)
    r = connect_session.post(url, data=json.dumps(url_export_config))
    r.raise_for_status()
    return r.json()


def export_dataset_as_avro(
    connect_info: Client, *, url: str, dataset_name: str, fs_type: Enum
) -> Union[JsonDict, bool]:
    """
    Takes a connect info object and writes the corresponding avro file to specified url for
    specified dataset. By default assumes HDFS but if local_fs is set to true writes to server
    file system.

    Args:
        connect_info: The connect client to use
        url: the location in the relevant file system to which to write the avro schema
        dataset_name: the name of the dataset
        fs_type: the remote filesystem type. Currently supports 'HDFS' and 'LOCAL'

    Returns:
        json returned by df-connects /urlExport/<hdfs/serverfs>/avroSchema endpoint

    Raises:
        ValueError: if using an unsupported type of file system
        HTTPError: if the call to export the dataset was unsuccessful
    """

    if fs_type == FileSystemType.LOCAL:
        api_path = f"/api/urlExport/{fs_type.value}/avro"
    elif fs_type == FileSystemType.HDFS:
        api_path = f"/api/urlExport/{fs_type.value}/avro"
    else:
        error = (
            f"trying to use unsupported type {fs_type}, supported are "
            f"'{FileSystemType.LOCAL.value}' and '{FileSystemType.HDFS.value}'"
        )
        LOGGER.error(error)
        raise ValueError(error)

    url_export_config = _get_avro_url_export_config(url, dataset_name)

    # establish a df_connect session and make API call
    connect_session = get_connect_session(connect_info)
    url = _get_url(connect_info, api_path)
    r = connect_session.post(url, data=json.dumps(url_export_config))
    r.raise_for_status()
    return r.json()
