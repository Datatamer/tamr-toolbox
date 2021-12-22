"""Tests for tasks related to connecting to a Tamr instance"""
import os
import pytest
import tempfile
from tamr_toolbox import utils
from tamr_unify_client.auth import UsernamePasswordAuth
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir


# Provide dummy default for offline tests
os.environ.setdefault(
    "TAMR_TOOLBOX_PASSWORD", "none_provided",
)

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/utils.config.yaml"
)


@mock_api()
def test_client_create():
    my_client = utils.client.create(**CONFIG["my_instance_name"])
    assert my_client.host == CONFIG["my_instance_name"]["host"]
    assert my_client.port == int(CONFIG["my_instance_name"]["port"])
    assert my_client.protocol == CONFIG["my_instance_name"]["protocol"]
    assert my_client.base_path == "/api/versioned/v1/"
    assert my_client.auth == UsernamePasswordAuth(
        "admin", os.environ["TAMR_TOOLBOX_PASSWORD"],
    )


@mock_api()
def test_client_create_none_port():
    my_client = utils.client.create(**CONFIG["my_portless_instance"])
    assert my_client.port is None


@mock_api()
def test_invalid_credentials():
    with pytest.raises(SystemError):
        utils.client.create(**CONFIG["my_other_instance"], enforce_healthy=True)


@mock_api()
def test_store_auth_cookie():
    my_client = utils.client.create(
        **CONFIG["my_instance_name"], store_auth_cookie=True
    )
    assert "authToken" in my_client.session.cookies.keys()
    assert my_client.session.auth is None


@mock_api()
def test_client_enforce_healthy():
    my_client = utils.client.create(**CONFIG["my_instance_name"], enforce_healthy=True)
    assert my_client.host == CONFIG["my_instance_name"]["host"]
    assert my_client.port == int(CONFIG["my_instance_name"]["port"])
    assert my_client.protocol == CONFIG["my_instance_name"]["protocol"]
    assert my_client.base_path == "/api/versioned/v1/"
    assert my_client.auth == UsernamePasswordAuth(
        "admin", os.environ["TAMR_TOOLBOX_PASSWORD"],
    )


@mock_api()
def test_get_with_connection_retry():
    # To ensure timeout error is raised to test correct ConnectionError is caught

    log_prefix = "caught_connection_error"
    log_file_path = os.path.join(
        tempfile.gettempdir(), f"{log_prefix}_{utils.logger._get_log_filename()}"
    )
    log_file_path_extended = os.path.join(
        log_file_path, utils.logger._get_log_filename()
    )  # because gettempdir creates a directory
    if os.path.exists(log_file_path_extended):
        os.remove(log_file_path_extended)

    my_client = utils.client.create(**CONFIG["my_other_instance"])
    utils.logger.enable_toolbox_logging(
        log_to_terminal=False, log_directory=log_file_path
    )

    with pytest.raises(TimeoutError):
        utils.client.get_with_connection_retry(
            my_client, "/api/service/health", timeout_seconds=10, sleep_seconds=1
        )

    assert "ConnectionError" not in open(log_file_path_extended).read()
