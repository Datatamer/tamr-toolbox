"""Tests for related to the Tamr auxiliary service DF-connect"""
import pytest
from tamr_toolbox.data_io.df_connect import client
from tamr_toolbox.utils.config import from_yaml
from tests._common import get_toolbox_root_dir

CONFIG = from_yaml(get_toolbox_root_dir() / "tests/mocking/resources/connect.config.yaml")
CONFIG_HTTPS = from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/connect_https.config.yaml"
)
CONFIG_MULTI_EXPORT = from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/connect_multi_export.config.yaml"
)


@pytest.mark.parametrize(
    "protocol,port,base_path, expected",
    [
        ("http", "9030", "", "http://localhost:9030/api/jdbc/ingest"),
        ("https", "9030", "", "https://localhost:9030/api/jdbc/ingest"),
        ("http", "", "", "http://localhost/api/jdbc/ingest"),
        ("https", "", "", "https://localhost/api/jdbc/ingest"),
        ("http", "", "/proxy", "http://localhost/proxy/api/jdbc/ingest"),
        ("https", "", "proxy", "https://localhost/proxy/api/jdbc/ingest"),
        ("http", "9030", "proxy", "http://localhost:9030/proxy/api/jdbc/ingest"),
        ("https", "9030", "proxy", "https://localhost:9030/proxy/api/jdbc/ingest"),
    ],
)
def test_create_with_multiple_parameters(protocol: str, port: str, base_path: str, expected: str):
    connect_info = client.create(
        host="localhost",
        port=port,
        protocol=protocol,
        tamr_username="",
        tamr_password="",
        base_path=base_path,
        jdbc_dict=CONFIG["df_connect"]["jdbc"]["ingest"],
    )
    assert expected == client._get_url(connect_info, "api/jdbc/ingest")


def test_create_bad_configuration():
    """Ensure a bad configuration raises a connection error """
    with pytest.raises(RuntimeError):
        my_connect = client.create(
            host="localhost",
            port="80",
            protocol="http",
            tamr_username="user",
            tamr_password="password",
            base_path="",
            jdbc_dict=CONFIG["df_connect"]["jdbc"]["ingest"],
        )
        client.get_connect_session(my_connect)


def test_get_url_http():
    my_connect = client.from_config(CONFIG)
    assert client._get_url(my_connect, "/api/jdbcIngest") == "http://localhost:9030/api/jdbcIngest"


def test_get_url_https():
    my_ssl_connect = client.from_config(CONFIG_HTTPS)
    assert (
        client._get_url(my_ssl_connect, "/api/jdbcIngest")
        == "https://localhost/df_connect/api/jdbcIngest"
    )


def test_get_export_config_default():
    test_export_config = {
        "mergedArrayValuesDelimiter": "|",
        "limitRecords": 0,
        "columnsExcludeRegex": "",
        "export_delta": False,
    }
    assert client._get_export_config() == test_export_config


def test_get_export_config_with_args():
    test_export_config = {
        "mergedArrayValuesDelimiter": ",",
        "limitRecords": 10,
        "columnsExcludeRegex": "(origin).*",
        "exportDelta": True,
        "deltaConfig": {"fromVersion": "100", "toVersion": "102"},
    }
    assert (
        client._get_export_config(",", 10, "(origin).*", True, "100", "102") == test_export_config
    )


def test_deployment_parsing():
    my_connect = client.from_config(CONFIG)
    assert my_connect.host == "localhost"
    assert my_connect.port == "9030"
    assert my_connect.protocol == "http"
    assert my_connect.base_path == ""
    assert my_connect.tamr_username == "my_user"
    assert my_connect.tamr_password == "my_password"


def test_jdbc_parsing():
    my_connect = client.from_config(CONFIG)
    assert my_connect.jdbc_info.jdbc_url == "tamr::jdbc_ingest"
    assert my_connect.jdbc_info.db_user == "ingest_user"
    assert my_connect.jdbc_info.db_password == "ingest_pw"
    assert my_connect.jdbc_info.fetch_size == 10000


def test_get_query_config_from_ingest():
    my_connect = client.from_config(CONFIG)
    ingest_info = my_connect.jdbc_info
    query_config = client._get_query_config(ingest_info)
    assert query_config["jdbcUrl"] == ingest_info.jdbc_url
    assert query_config["dbUsername"] == ingest_info.db_user
    assert query_config["dbPassword"] == ingest_info.db_password
    assert query_config["fetchSize"] == ingest_info.fetch_size


def test_https_deployment_processing():
    my_connect = client.from_config(CONFIG_HTTPS)
    assert my_connect.protocol == "https"
    assert my_connect.port == ""
    assert my_connect.base_path == "df_connect"


def test_oracle_parsing():
    my_connect = client.from_config(CONFIG_MULTI_EXPORT, jdbc_key="oracle",)
    assert my_connect.jdbc_info.jdbc_url == "jdbc::oracle_db"
    assert my_connect.jdbc_info.db_user == "oracle_user"
    assert my_connect.jdbc_info.db_password == "oracle_pw"


def test_postgres_parsing():
    my_connect = client.from_config(CONFIG_MULTI_EXPORT, jdbc_key="postgres",)
    assert my_connect.jdbc_info.jdbc_url == "jdbc::postgres_db"
    assert my_connect.jdbc_info.db_user == "postgres_user"
    assert my_connect.jdbc_info.db_password == "postgres_pw"
