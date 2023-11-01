"""Tasks related to interacting with the Tamr Core-connect"""
from dataclasses import dataclass
import json
import logging
import requests
from tamr_toolbox.data_io.core_connect import jdbc_info
from tamr_toolbox.models.data_type import JsonDict
from tamr_unify_client.auth import UsernamePasswordAuth
from typing import Dict, List, Optional

LOGGER = logging.getLogger(__name__)


@dataclass
class Client:
    """A data class for interacting with core_connect via jdbc.

    Args:
        host: the host where core_connect is running
        port: the port on which core_connect is listening
        protocol: http or https
        base_path: if using nginx-like proxy this is the redirect path
        tamr_username: the tamr account to use
        tamr_password: the password for the tamr account to use
        jbdc_info: configuration information for the jdbc connection
        cert: optional path to a certfile for authentication
    """

    host: str
    port: str
    protocol: str
    base_path: str
    tamr_username: str
    tamr_password: str
    jdbc_info: jdbc_info.JdbcInfo
    cert: Optional[str]


def from_config(
    config: JsonDict, config_key: str = "core_connect", jdbc_key: str = "ingest"
) -> Client:
    """Constructs a Client object from a json dictionary.

    Args:
        config: A json dictionary of configuration values
        config_key: block of the config to parse for values. Defaults to 'core_connect'
        jdbc_key: the key used to specify which block of core_connect-->jdbc in configuration to be
            used for picking up database connection information. Defaults to 'ingest'

    Returns:
        A Client object
    """

    # proxy and port redirect are optional
    base_path = config[config_key].get("base_path", "")
    port = config[config_key].get("port", "")

    # Optional cert may or may not be present in config file (back-compat from TBOX-295)
    if "cert" in config[config_key].keys():
        cert = config[config_key]["cert"]
    else:
        cert = None

    return Client(
        host=config[config_key]["host"],
        port=port,
        protocol=config[config_key]["protocol"],
        base_path=base_path,
        tamr_username=config[config_key]["tamr_username"],
        tamr_password=config[config_key]["tamr_password"],
        jdbc_info=jdbc_info.from_config(config, config_key=config_key, jdbc_key=jdbc_key),
        cert=cert,
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
    cert: Optional[str] = None,
) -> Client:
    """
    Simple wrapper for creating an instance of `Client` dataclass object.

    Args:
        host: the host where core_connect is running
        port: the port on which core_connect is listening
        protocol: http or https
        base_path: if using nginx-like proxy this is the redirect path
        tamr_username: the tamr account to use
        tamr_password: the password for the tamr account to use
        jdbc_dict: configuration information for the jdbc connection
        cert: optional path to a certfile for authentication

    Returns:
        An instance of `tamr_toolbox.data_io.core_connect.Client`
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
        cert=cert,
    )


def _get_url(connect_info: Client, api_path: str) -> str:
    """Constructs and returns url for request to core_connect. Valid for both http and https.

     Args:
         connect_info: An object of type JdbcInfo from which to pull host/port
         api_path: The api endpoint to be called

    Returns:
        url: A string of the request url formatted correctly for that instance of core_connect.
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


def _get_query_config(jdbc_info: jdbc_info.JdbcInfo) -> Dict:
    """Packages configuration info into relevant query configuration json (dict) which is used
    for multiple core_connect API calls.

    Args:
        jdbc_info: JdbcInfo object from which to construct the query configuration.

    Returns:
        query_config: A dictionary suitable for usage in all core_connect API calls needing a
            queryConfig parameter
    """
    query_config = {
        "jdbcUrl": jdbc_info.jdbc_url,
        "dbUsername": jdbc_info.db_user,
        "dbPassword": jdbc_info.db_password,
        "fetchSize": jdbc_info.fetch_size,
    }
    return query_config


def get_connect_session(connect_info: Client) -> requests.Session:
    """Returns an authenticated session using Tamr credentials from configuration.
    Raises an exception if core_connect is not installed or running correctly.

    Args:
        connect_info: An instance of a Client object

    Returns:
        An authenticated session

    Raises:
        RuntimeError: if a connection to core_connect cannot be established
    """
    auth = UsernamePasswordAuth(connect_info.tamr_username, connect_info.tamr_password)
    s = requests.Session()
    s.auth = auth
    s.headers.update({"Content-type": "application/json"})
    s.headers.update({"Accept": "application/json"})
    s.cert = connect_info.cert

    # test that df_connect is running properly
    url = _get_url(connect_info, "/api/service/health")
    try:
        r = s.get(url)
        r.raise_for_status()
    except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError):
        raise RuntimeError(
            f"Tamr auxiliary service core_connect is either not running or not healthy at {url}!"
            f" Did you install it? Core-connect does not come with default Tamr installation."
            f" Check its status and your configuration."
        )
    return s


def ingest_dataset(
    connect_info: Client, *, dataset_name: str, query: str, primary_key=None
) -> JsonDict:
    """
    Ingest a dataset into Tamr via Core-connect given dataset name, query string, and optional
    list of columns for primary key

    Args:
        dataset_name: Name of dataset
        query: jdbc query to execute in the database and results of which will be loaded into Tamr
        connect_info: A Client object for establishing session and loading jdbc parameters
        primary_key: list of columns to use as primary key. If None then Core_connect will generate
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


def profile_query_results(
    connect_info: Client, *, dataset_name: str, queries: List[str]
) -> JsonDict:
    """
    Profile the contents of JDBC queries via core_connect and write results to a Tamr dataset.
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
