"""Tests for tasks related to the version of Tamr instances"""
import pytest
from typing import Union, Type

from tamr_toolbox.utils import version, config, client
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = config.from_yaml(get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml")


@pytest.mark.parametrize(
    "version_string, expected_output",
    [("0.43.0", 0 + 43.0), ("2020.015.0", (2020 * 1000) + 15.0)],
)
def test__as_float(version_string: str, expected_output: float):
    assert version._as_float(version_string) == expected_output


@pytest.mark.parametrize(
    "version_string, error_class",
    [
        ("43", ValueError),
        ("v2019.023.2", ValueError),
        ("develop-SNAPSHOT", ValueError),
        (43, AttributeError),
    ],
)
def test__as_float_invalid_input(version_string: Union[str, int], error_class: Type["Exception"]):
    with pytest.raises(error_class):
        version._as_float(version_string)


@pytest.mark.parametrize(
    "smaller, larger",
    [
        ("0.43.0", "0.44.0"),
        ("0.43.0", "2019.001.0"),
        ("2019.023.1", "2019.023.2"),
        ("2019.023.1", "2020.001.0"),
        ("2020.001.0", "2020.002.0"),
    ],
)
def test__as_float_ordering(smaller: str, larger: str):
    assert version._as_float(smaller) < version._as_float(larger)


@pytest.mark.parametrize(
    "required_version, expected_pass_enforcement", [("0.40.0", True), ("2050.001.0", False)]
)
@mock_api()
def test_enforce_after_or_equal(required_version: str, expected_pass_enforcement: bool):
    tamr = client.create(**CONFIG["toolbox_test_instance"])
    try:
        version.enforce_after_or_equal(tamr, compare_version=required_version)
        assert expected_pass_enforcement is True
    except NotImplementedError:
        assert expected_pass_enforcement is False
