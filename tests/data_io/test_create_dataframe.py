"""Tests for tasks related to moving data in or out of Tamr using pandas.Dataframes"""
import pytest

from tamr_toolbox.data_io import dataframe
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

# A valid password is not needed for offline tests, some value must be provided
CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)
GR_DATASET_ID = CONFIG["datasets"]["minimal_golden_records_golden_records"]
SM_DATASET_ID = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
INPUT_DATASET_ID = CONFIG["datasets"]["people_tiny_copy"]
UNSTREAMABLE_DATASET_ID = CONFIG["datasets"]["broken_schema_mapping_unified_dataset"]


@mock_api()
def test_create_dataframe():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    df = dataframe.from_dataset(dataset)
    df = df.set_index("tamr_id")

    assert df.shape == (18, 8)
    assert df.loc["-8652805551987624164", "all_names"] == ["Tuck", "Tucker"]
    assert df.loc["-8652805551987624164", "first_name"] == ["Tucker"]
    assert df.loc["-8652805551987624164", "ssn"] == [""]


@mock_api()
def test_create_dataframe_then_flatten():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    df = dataframe.from_dataset(dataset)
    df1 = dataframe.flatten(df, delimiter="||")
    df1 = df1.set_index("tamr_id")

    assert df1.shape == (18, 8)
    assert df1.loc["-8652805551987624164", "all_names"] == "Tuck||Tucker"
    assert df1.loc["-8652805551987624164", "first_name"] == "Tucker"
    assert df1.loc["-8652805551987624164", "ssn"] == ""

    df2 = dataframe.flatten(df, delimiter="||", columns=["first_name", "ssn"])
    df2 = df2.set_index("tamr_id")

    assert df2.shape == (18, 8)
    assert df2.loc["-8652805551987624164", "all_names"] == ["Tuck", "Tucker"]
    assert df2.loc["-8652805551987624164", "first_name"] == "Tucker"
    assert df2.loc["-8652805551987624164", "ssn"] == ""


@mock_api()
def test_create_dataframe_flattened():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(GR_DATASET_ID)
    df = dataframe.from_dataset(dataset, flatten_delimiter="||")
    df = df.set_index("persistentId")

    assert df.shape == (8, 9)
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "all_first_names"] == "Rob||Robert"
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "name_lengths"] == [3, 6]
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "ssn"] == "123"
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "Cluster Size"] == 2


@mock_api()
def test_create_dataframe_flattened_columns():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    df = dataframe.from_dataset(
        dataset, flatten_delimiter="||", flatten_columns=["first_name", "ssn"]
    )
    df = df.set_index("tamr_id")

    assert df.shape == (18, 8)
    assert df.loc["-8652805551987624164", "all_names"] == ["Tuck", "Tucker"]
    assert df.loc["-8652805551987624164", "first_name"] == "Tucker"
    assert df.loc["-8652805551987624164", "ssn"] == ""


@mock_api()
def test_create_dataframe_force_flattened():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(GR_DATASET_ID)
    df = dataframe.from_dataset(dataset, flatten_delimiter="||", force_flatten=True)
    df = df.set_index("persistentId")

    assert df.shape == (8, 9)
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "all_first_names"] == "Rob||Robert"
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "name_lengths"] == "3||6"
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "ssn"] == "123"
    assert df.loc["218c3f66-b240-3b08-b688-2c8d0506f12f", "Cluster Size"] == 2


@mock_api()
def test_create_dataframe_force_flatten_no_delimiter():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(GR_DATASET_ID)
    with pytest.raises(ValueError):
        dataframe.from_dataset(dataset, force_flatten=True)


@mock_api()
def test_create_dataframe_nrows():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    df = dataframe.from_dataset(dataset, nrows=5)
    df = df.set_index("tamr_id")

    assert df.shape == (5, 8)


@mock_api()
def test_create_dataframe_columns():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    df = dataframe.from_dataset(dataset, columns=["tamr_id", "last_name", "first_name"])
    df = df.set_index("tamr_id")

    assert df.shape == (18, 2)
    assert list(df.columns) == ["last_name", "first_name"]
    assert df.loc["-8652805551987624164", "first_name"] == ["Tucker"]


@mock_api()
def test_create_dataframe_wrong_columns():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(SM_DATASET_ID)
    with pytest.raises(ValueError):
        dataframe.from_dataset(dataset, columns=["tamr_id", "middle_initial"])


@mock_api()
def test_create_dataframe_unstreamable():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset = client.datasets.by_resource_id(INPUT_DATASET_ID)
    df_input = dataframe.from_dataset(input_dataset)
    input_dataset.upsert_from_dataframe(
        df_input.head(1), primary_key_name=input_dataset.key_attribute_names[0],
    )
    dataset = client.datasets.by_resource_id(UNSTREAMABLE_DATASET_ID)
    with pytest.raises(RuntimeError):
        dataframe.from_dataset(dataset)


@mock_api()
def test_create_dataframe_refresh():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    input_dataset = client.datasets.by_resource_id(INPUT_DATASET_ID)
    df_input = dataframe.from_dataset(input_dataset)
    input_dataset.upsert_from_dataframe(
        df_input.head(1), primary_key_name=input_dataset.key_attribute_names[0],
    )
    dataset = client.datasets.by_resource_id(UNSTREAMABLE_DATASET_ID)
    df = dataframe.from_dataset(dataset, allow_dataset_refresh=True)
    df = df.set_index("tamr_id")

    assert df.shape == (9, 8)
    assert df.loc["-1366726601913727714", "first_name"] == ["Jeff"]
