"""Tasks related to handling jdbc information for the Tamr auxiliary service DF-connect"""
from dataclasses import dataclass
from tamr_toolbox.models.data_type import JsonDict


@dataclass
class JdbcInfo:
    """
    A dataclass to tie together relevant data to ingest data into df_connect.

    Args:
        jdbc_url: The jdbc url for the database to which to connect
        db_user: The database user as whom to authenticate
        db_password: The password for the database user
        fetch_size: The number of records by which to chunk the jdbc ResultSet
    """

    jdbc_url: str
    db_user: str
    db_password: str
    fetch_size: int


def from_config(
    config: JsonDict, *, config_key: str = "df_connect", jdbc_key: str = "ingest"
) -> JdbcInfo:
    """Create an instance of JdbcInfo from a json object.

    Args:
        config: A json dictionary containing configuration values
        config_key: the top-level key of the config to use.
        jdbc_key: the key to use for the jdbc block. Needs to be within `config_key` block.
                  Defaults to 'ingest', but can be used to specify any sub-block of a config
                  object or yaml file. See example configs and exports for more context.
    """
    return JdbcInfo(
        jdbc_url=config[config_key]["jdbc"][jdbc_key]["jdbc_url"],
        db_user=config[config_key]["jdbc"][jdbc_key]["db_user"],
        db_password=config[config_key]["jdbc"][jdbc_key]["db_password"],
        fetch_size=config[config_key]["jdbc"][jdbc_key]["fetch_size"],
    )


def create(*, jdbc_url: str, db_user: str, db_password: str, fetch_size: int) -> JdbcInfo:
    """
    A simple wrapper to create an object of type `JdbcInfo`

    Args:
        jdbc_url: The jdbc url for the database to which to connect
        db_user: The database user as whom to authenticate
        db_password: The password for the database user
        fetch_size: The number of records by which to chunk the jdbc ResultSet

    Returns:
        An instance of a JdbcInfo object.
    """
    return JdbcInfo(
        jdbc_url=jdbc_url, db_user=db_user, db_password=db_password, fetch_size=fetch_size
    )
