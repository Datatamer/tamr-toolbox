"""Tests for tasks related to loading and using configuration files"""
import os
from pathlib import Path
import pytest
import tamr_toolbox.utils.config
from tests._common import get_toolbox_root_dir


def test_from_yaml():
    # ensure values are set to None when we aren't setting environment variables
    env_not_set = tamr_toolbox.utils.config.from_yaml(
        get_toolbox_root_dir() / "tests/mocking/resources/environment_variables.config.yaml"
    )
    assert env_not_set["my_instance_name"]["host"] is None
    assert env_not_set["my_instance_name"]["username"] is None
    assert env_not_set["my_instance_name"]["password"] is None

    # set needed environment variables and ensure we read them in correctly
    os.environ["TAMR_MY_INSTANCE_HOST"] = "localhost"
    os.environ["TAMR_MY_INSTANCE_USERNAME"] = "user"
    os.environ["TAMR_MY_INSTANCE_PASSWORD"] = "password"

    my_config = tamr_toolbox.utils.config.from_yaml(
        get_toolbox_root_dir() / "tests/mocking/resources/environment_variables.config.yaml"
    )
    assert my_config["my_instance_name"]["host"] == "localhost"
    assert my_config["my_instance_name"]["username"] == "user"
    assert my_config["my_instance_name"]["password"] == "password"

    # Check that strings starting with a $ are interpreted as a strings
    assert my_config["price"] == "$12"
    assert my_config["price_words"] == "$twelve"


def test_from_yaml_with_default():
    os.environ["TAMR_TOOLBOX_PASSWORD"] = "password"

    # Test using target file path, even when default path is populated
    my_config_1 = tamr_toolbox.utils.config.from_yaml(
        path_to_file=get_toolbox_root_dir() / "tests/mocking/resources/utils.config.yaml",
        default_path_to_file="/not/a/real/path",
    )
    assert my_config_1["my_other_instance"]["host"] == "1.2.3.4"

    # Test default path when target path is None
    my_config_2 = tamr_toolbox.utils.config.from_yaml(
        path_to_file=None,
        default_path_to_file=get_toolbox_root_dir() / "tests/mocking/resources/utils.config.yaml",
    )
    assert my_config_2["my_other_instance"]["host"] == "1.2.3.4"

    # Test default path when target path is empty
    my_config_3 = tamr_toolbox.utils.config.from_yaml(
        path_to_file="",
        default_path_to_file=get_toolbox_root_dir() / "tests/mocking/resources/utils.config.yaml",
    )
    assert my_config_3["my_other_instance"]["host"] == "1.2.3.4"

    # Expect failure when both are empty
    with pytest.raises(ValueError):
        tamr_toolbox.utils.config.from_yaml(path_to_file="", default_path_to_file="")


def test_from_yaml_with_path():
    os.environ["TAMR_TOOLBOX_PASSWORD"] = "password"

    # Test using target file path as a Path object
    my_config_1 = tamr_toolbox.utils.config.from_yaml(
        path_to_file=Path(
            get_toolbox_root_dir(), "tests", "mocking", "resources", "utils.config.yaml"
        ),
        default_path_to_file=Path("not", "a", "real", "path"),
    )
    assert my_config_1["my_other_instance"]["host"] == "1.2.3.4"

    # Test default path when target path is None
    my_config_2 = tamr_toolbox.utils.config.from_yaml(
        path_to_file=None,
        default_path_to_file=Path(
            get_toolbox_root_dir(), "tests", "mocking", "resources", "utils.config.yaml"
        ),
    )
    assert my_config_2["my_other_instance"]["host"] == "1.2.3.4"
