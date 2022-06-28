"""Tests for RealTime match utilities"""
from logging import warning
from typing import Optional

import pytest

from tamr_toolbox import utils
from tamr_toolbox.realtime.matching import (
    match_query,
    poll_realtime_match_status,
    update_realtime_match_data,
)
from tamr_toolbox.utils.testing import mock_api
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
    "type, batch_size, primary_key",
    [
        ("records", 1000, None),
        ("clusters", 1000, None),
        ("records", 1, None),
        ("clusters", 1, None),
        ("records", 1000, "testkey"),
        ("clusters", 1000, "testkey"),
        ("records", 1, "testkey"),
        ("clusters", 1, "testkey"),
    ],
)
@mock_api()
def test_match_query_with_match(type: str, batch_size: int, primary_key: Optional[str]):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])

    data = (
        MATCH_TEST_DATA
        if primary_key is None
        else [{**d, primary_key: f"rec{k}"} for k, d in enumerate(MATCH_TEST_DATA)]
    )

    result = match_query(
        match_client=match_client,
        project=project,
        records=data,
        type=type,
        batch_size=batch_size,
        primary_key=primary_key,
    )

    if type == "records":
        assert {"7279808247767404449", "8878137442375545950"} == {
            x["matchedRecordId"] for x in result["rec0" if primary_key else 0]
        }
        assert {"-198958353428908929", "1134804050832671496"} == {
            x["matchedRecordId"] for x in result["rec1" if primary_key else 1]
        }
    else:
        assert {"218c3f66-b240-3b08-b688-2c8d0506f12f"} == {
            x["clusterId"] for x in result["rec0" if primary_key else 0]
        }
        assert {
            "565e03e5-9349-34ef-a779-c4bcd9dcc49c",
            "8762d70e-b8a5-39f8-a387-8c9148e8254f",
        } == {x["clusterId"] for x in result["rec1" if primary_key else 1]}
    assert result["rec2" if primary_key else 2]
    assert result["rec3" if primary_key else 3]
    assert result["rec4" if primary_key else 4] == []  # testing that we don't get an index error

    return None


@pytest.mark.parametrize("type", ["records", "clusters"])
@mock_api()
def test_match_query_with_no_match(type: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    result = match_query(
        match_client=match_client, project=project, records=[{"ssn": "0000"}], type=type,
    )
    assert result[0] == []
    return None


@mock_api()
def test_match_bad_batch_size():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    with pytest.raises(ValueError):
        match_query(
            match_client=match_client,
            project=project,
            records=[{"ssn": "0000"}],
            type="records",
            batch_size=-1,
        )
    return None


@mock_api()
def test_match_bad_request_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    with pytest.raises(ValueError):
        match_query(
            match_client=match_client,
            project=project,
            records=[{"ssn": "0000"}],
            type="unknown type",
        )
    return None


@mock_api()
def test_match_no_input_data():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])

    with pytest.warns(
        expected_warning=warning("No input supplied to match_query -- returning empty result.")
    ):
        result = match_query(
            match_client=match_client, project=project, records=[], type="records",
        )
    assert len(result) == 0
    return None


@mock_api()
def test_poll_match_status():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    # Test project that is queryable
    queryable = poll_realtime_match_status(match_client=match_client, project=project)
    assert queryable
    # Test project that is not queryable
    project = client.projects.by_name("minimal_incomplete_mastering")
    queryable = poll_realtime_match_status(match_client=match_client, project=project)
    assert ~queryable
    return None


@mock_api()
def test_match_query_nonqueryable_project():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_incomplete_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    with pytest.raises(RuntimeError):
        match_query(
            match_client=match_client,
            project=project,
            records=[
                {
                    "ssn": [""],
                    "last_name": ["Cohen"],
                    "first_name": ["Rob"],
                    "all_names": ["", "Rob"],
                    "full_name": ["Rob Cohen"],
                }
            ],
            type="records",
        )
    return None


@pytest.mark.parametrize("type", ["records", "clusters"])
@mock_api()
def test_match_max_num_matches(type: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    result = match_query(
        match_client=match_client,
        project=project,
        records=MATCH_TEST_DATA[:2],
        type=type,
        max_num_matches=1,
    )

    if type == "records":
        assert {"7279808247767404449"} == {x["matchedRecordId"] for x in result[0]}
        assert {"-198958353428908929"} == {x["matchedRecordId"] for x in result[1]}
    elif type == "clusters":
        assert {"218c3f66-b240-3b08-b688-2c8d0506f12f"} == {x["clusterId"] for x in result[0]}
        assert {"565e03e5-9349-34ef-a779-c4bcd9dcc49c"} == {x["clusterId"] for x in result[1]}

    return None


@pytest.mark.parametrize("type", ["records", "clusters"])
@mock_api()
def test_match_min_match_prob(type: str):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    result = match_query(
        match_client=match_client,
        project=project,
        records=MATCH_TEST_DATA,
        type=type,
        min_match_prob=0.2,
    )

    full_result = match_query(
        match_client=match_client, project=project, records=MATCH_TEST_DATA, type=type,
    )

    assert full_result[0] == result[0]  # same results for cases with all matches above 0.2 prob
    # For record 2, get nothing from probability-filtered call; get responses from full call
    assert result[2] == []
    if type == "records":
        assert "-3811118809423689344" in {x["matchedRecordId"] for x in full_result[2]}
    elif type == "clusters":
        assert "86bd93c3-ceff-3174-9125-da0c33356426" in {x["clusterId"] for x in full_result[2]}

    return None


@mock_api()
def test_update_match_data_bad_proj():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_categorization")
    with pytest.raises(RuntimeError, match="failed at submission time"):
        update_realtime_match_data(project=project)
    return None


@mock_api()
def test_match_bad_primary_key():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_name("minimal_mastering")
    match_client = utils.client.create(**CONFIG["toolbox_realtime_match_instance"])
    records = MATCH_TEST_DATA[1:3]
    records[0]["test_primary_key"] = "samplekey"

    with pytest.raises(ValueError, match="Not all input records had a primary key"):
        match_query(
            match_client=match_client,
            project=project,
            records=records,
            type="records",
            primary_key="test_primary_key",
        )
    return None
