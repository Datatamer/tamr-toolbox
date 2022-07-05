"""Tests for tasks related to the version of Tamr instances"""
from typing import Union, Type

import pytest

from tamr_toolbox.utils import version, config, client
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


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
def test__as_float_invalid_input(
    version_string: Union[str, int], error_class: Type["Exception"]
):
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
    "required_version, expected_pass_enforcement",
    [("0.40.0", True), ("2050.001.0", False)],
)
@mock_api()
def test_enforce_after_or_equal(required_version: str, expected_pass_enforcement: bool):
    tamr = client.create(**CONFIG["toolbox_test_instance"])
    try:
        version.enforce_after_or_equal(tamr, compare_version=required_version)
        assert expected_pass_enforcement is True
    except NotImplementedError:
        assert expected_pass_enforcement is False


def test_is_version_condition_met():
    assert version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2022.002.0", exact_version=True
    )
    assert not version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2023.002.0", exact_version=True
    )
    assert not version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2021.002.0", exact_version=True
    )

    assert version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2021.002.0"
    )
    assert not version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2023.002.0"
    )

    assert version.does_tamr_version_meet_requirement(
        tamr_version="2022.002.0", min_version="2021.002.0", max_version="2023.002.0"
    )
    assert not version.does_tamr_version_meet_requirement(
        tamr_version="2020.002.0", min_version="2021.002.0", max_version="2023.002.0"
    )
    assert not version.does_tamr_version_meet_requirement(
        tamr_version="2024.002.0", min_version="2021.002.0", max_version="2023.002.0"
    )

    with pytest.raises(ValueError):
        version.does_tamr_version_meet_requirement(
            tamr_version="2019.003.0",
            min_version="2021.003.0",
            max_version="2019.003.0",
        )

    with pytest.raises(EnvironmentError):
        version.does_tamr_version_meet_requirement(
            tamr_version="2022.002.0",
            min_version="2021.002.0",
            exact_version=True,
            raise_error=True,
        )


@mock_api()
def test_get_tamr_versions_from_function_args():
    tamr_client = client.create(**CONFIG["toolbox_test_instance"])
    tamr_project = tamr_client.projects.by_resource_id(
        CONFIG["projects"]["minimal_mastering"]
    )
    tamr_dataset = tamr_client.datasets.by_resource_id(
        CONFIG["datasets"]["groceries_tiny"]
    )

    assert (
        version._get_tamr_versions_from_function_args(5, "words", a=["other_types"])
        == []
    )

    assert version._get_tamr_versions_from_function_args(tamr_client) == [
        version.current(tamr_client)
    ]
    assert version._get_tamr_versions_from_function_args(tamr_project) == [
        version.current(tamr_client)
    ]
    assert version._get_tamr_versions_from_function_args(tamr_dataset) == [
        version.current(tamr_client)
    ]

    assert version._get_tamr_versions_from_function_args(
        tamr_client, tamr_project, tamr_dataset, "a"
    ) == [
        version.current(tamr_client),
        version.current(tamr_client),
        version.current(tamr_client),
    ]


@mock_api()
def test_ensure_tamr_version_decorator():
    tamr_client = client.create(**CONFIG["toolbox_test_instance"])

    try:

        @version.requires_tamr_version(min_version="2019.001")
        def test_function_irrelevant_inputs(*args, **kwargs):
            pass

        test_function_irrelevant_inputs(1, "2", three="stuff")
    except Exception as exc:
        assert False, f"Raised an exception {exc}"

    try:

        @version.requires_tamr_version(min_version="2019.001")
        def test_function_args_passing(tamr_client, *args, **kwargs):
            pass

        test_function_args_passing(tamr_client)
    except Exception as exc:
        assert False, f"Raised an exception {exc}"

    with pytest.raises(EnvironmentError):

        @version.requires_tamr_version(min_version="2099.001")
        def test_function_args_failing(tamr_client, *args, **kwargs):
            pass

        test_function_args_failing(tamr_client)
