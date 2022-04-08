"""Tests for tasks related to moving data in or out of Tamr using delimited files"""
import pytest

from tamr_toolbox import utils
from tamr_toolbox.utils.testing import mock_api
from tamr_toolbox.low_latency.llm import llm_query

from tests._common import get_toolbox_root_dir


CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


MATCH_TEST_DATA = [
    {
        "ssn": [""],
        "last_name": ["Cohen"],
        "first_name": ["Rob"],
        "all_names": ["", "Rob"],
        "full_name": ["Rob Cohen"],
    },
    {"ssn": ["456"], "last_name": ["Ames"], "first_name": ["Jen"]},
    {"ssn": ["999"], "first_name": ["Ben"], "all_names": ["", "Ben"], "full_name": ["Ben Brown"]},
    {
        "ssn": ["123"],
        "last_name": ["Cohen"],
        "first_name": ["Robert"],
        "all_names": ["Rob", "Robert"],
        "full_name": ["Robert Cohen"],
    },
]


@pytest.mark.parametrize(
    "type, batch_size", [("records", 1000), ("clusters", 1000), ("records", 1), ("clusters", 1)]
)
@mock_api()
def test_llm_query_with_match(type: str, batch_size: int):
    llm_client = utils.client.create(**CONFIG["toolbox_llm_test_instance"])
    result = llm_query(
        llm_client,
        project_name="minimal_mastering",
        records=MATCH_TEST_DATA,
        type=type,
        batch_size=batch_size,
    )

    if type == "records":
        assert {"7279808247767404449", "8878137442375545950"} == {
            x["matchedRecordId"] for x in result[0]
        }
        assert {"-198958353428908929", "1134804050832671496"} == {
            x["matchedRecordId"] for x in result[1]
        }
    else:
        assert {"218c3f66-b240-3b08-b688-2c8d0506f12f"} == {x["clusterId"] for x in result[0]}
        assert {
            "565e03e5-9349-34ef-a779-c4bcd9dcc49c",
            "8762d70e-b8a5-39f8-a387-8c9148e8254f",
        } == {x["clusterId"] for x in result[1]}
    assert result[2]
    assert result[3]
    assert result[4] == []

    return None


@pytest.mark.parametrize("type", ["records", "clusters"])
@mock_api()
def test_llm_query_with_no_match(type: str):
    llm_client = utils.client.create(**CONFIG["toolbox_llm_test_instance"])
    result = llm_query(
        llm_client, project_name="minimal_mastering", records=[{"ssn": "0000"}], type=type,
    )
    assert result[0] == []
    return None


def test_llm_bad_batch_size():
    llm_client = utils.client.create(**CONFIG["toolbox_llm_test_instance"])
    with pytest.raises(ValueError):
        llm_query(
            llm_client,
            project_name="minimal_mastering",
            records=[{"ssn": "0000"}],
            type="records",
            batch_size=-1,
        )
    return None


def test_llm_no_input_data():
    llm_client = utils.client.create(**CONFIG["toolbox_llm_test_instance"])

    result = llm_query(llm_client, project_name="minimal_mastering", records=[], type="records",)
    assert len(result) == 0
    return None


@pytest.mark.parametrize("type", ["records", "clusters"])
@mock_api()
def test_llm_query_with_single_record(type: str):
    llm_client = utils.client.create(**CONFIG["toolbox_llm_test_instance"])
    result = llm_query(
        llm_client,
        project_name="minimal_mastering",
        records={
            "ssn": [""],
            "last_name": ["Cohen"],
            "first_name": ["Rob"],
            "all_names": ["", "Rob"],
            "full_name": ["Rob Cohen"],
        },
        type=type,
    )
    if type == "records":
        assert {"7279808247767404449", "8878137442375545950"} == {
            x["matchedRecordId"] for x in result[0]
        }
    else:
        assert {"218c3f66-b240-3b08-b688-2c8d0506f12f"} == {x["clusterId"] for x in result[0]}
    return None
