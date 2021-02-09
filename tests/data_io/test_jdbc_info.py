"""Tests for handling jdbc information for the Tamr auxiliary service DF-connect"""

from tamr_toolbox.data_io.df_connect import jdbc_info
from tamr_toolbox.utils import config
from tests._common import get_toolbox_root_dir

CONFIG = config.from_yaml(get_toolbox_root_dir() / "tests/mocking/resources/connect.config.yaml")


def test_from_json():
    my_jdbc_info = jdbc_info.from_config(CONFIG)
    assert my_jdbc_info.jdbc_url == "tamr::jdbc_ingest"
    assert my_jdbc_info.db_user == "ingest_user"
    assert my_jdbc_info.db_password == "ingest_pw"
