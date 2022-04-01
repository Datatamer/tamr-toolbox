"""Tests for tasks related to moving data in or out of Tamr using delimited files"""
import pytest
from typing import List, Optional, Dict
import io
import os
import tempfile

from tamr_toolbox.data_io import csv
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir
import pandas as pd
from datetime import datetime
from pathlib import Path

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)

# Raw export of minimal_schema_mapping_unified_dataset
TEST_DATA = (
    '"origin_source_name","origin_entity_id","tamr_id","first_name","last_name","ssn",'
    '"all_names","full_name","fullname","id","product_description"\n'
    '"employees_tiny.csv","C","4020443041522472195","Rob","Cohen","","|Rob","Rob Cohen","",'
    '"C",""\n'
    '"employees_tiny.csv","B","6600324236876171450","Bobby","Cohen","123","|Bobby",'
    '"Bobby Cohen","","B",""\n'
    '"employees_tiny.csv","D","4137835413832538851","Jen","Ames","456","|Jen","Jen Ames","",'
    '"D",""\n'
    '"people_tiny.csv","1","7279808247767404449","Robert","Cohen","123","Rob","Robert Cohen",'
    '"Rob","",""\n'
    '"employees_tiny.csv","A","8285661133131625474","Robert","Cohen","123","Rob|Robert",'
    '"Robert Cohen","","A",""\n'
    '"people_tiny.csv","4","1134804050832671496","Jen","Ames","456","","Jen Ames","","",""\n'
    '"people_tiny.csv","6","5971588015748314062","Ben","Brown","999","","Ben Brown","","",""\n'
    '"people_tiny.csv","7","-3811118809423689344","Jeff","Johnson","999","","Jeff Johnson",'
    '"","",""\n'
    '"employees_tiny.csv","H","-8652805551987624164","Tucker","Smith","","Tuck|Tucker",'
    '"Tucker Smith","","H",""\n'
    '"employees_tiny.csv","E","-6488501071167098470","Jennifer","Ames","456","|Jennifer",'
    '"Jennifer Ames","","E",""\n'
    '"people_tiny.csv","8","5622333943264804173","Ticker","Smith","","Tuck","Ticker Smith",'
    '"Tuck","",""\n'
    '"employees_tiny.csv","G","859925433232098763","Jeff","Johnson","999","|Jeff",'
    '"Jeff Johnson","","G",""\n'
    '"people_tiny.csv","3","8878137442375545950","Rob","Cohen","","","Rob Cohen","","",""\n'
    '"employees_tiny.csv","I","9058281253811394926","Frank","Zappa","553","|Frank",'
    '"Frank Zappa","","I",""\n'
    '"people_tiny.csv","9","3434387435204748789","Frank","Zappa","553","","Frank Zappa","","",""\n'
    '"people_tiny.csv","5","-198958353428908929","Jennifer","Ames","456","","Jennifer Ames",'
    '"","",""\n'
    '"people_tiny.csv","2","2682747759948153217","Bobby","Cohen","123","","Bobby Cohen","","",""\n'
    '"employees_tiny.csv","F","-7759189025239954849","Ben","Brown","999","|Ben","Ben Brown",'
    '"","F",""'
)

EMPTY_TEST_DATA = """
"id","first_name","nickname","last_name","ssn"
"""

TAXONOMY_DATA = [
    "Dairy\n",
    "Dairy,Cheese\n",
    "Dairy,Milk\n",
    "Meat\n",
    "Meat,Beef\n",
    "Meat,Chicken\n",
    "Meat,Chicken,bone-in\n",
    "Meat,Chicken,boneless\n",
    "Vegetables\n",
]


@pytest.mark.parametrize(
    "buffer_size, nrows, csv_delimiter, flatten_delimiter, columns",
    [
        (1, None, ",", "|", None),
        (2, None, ",", "|", None),
        (3, None, ",", "|", None),
        (5, None, ",", "|", None),
        (10, None, ",", "|", None),
        (100, None, ",", "|", None),
        (None, None, ",", "|", None),
        (100, 1, ",", "|", None),
        (100, 2, ",", "|", None),
        (100, 4, ",", "|", None),
        (100, 5, ",", "|", None),
        (100, 10, ",", "|", None),
        (None, None, ";", "|", None),
        (None, None, "|", ",", None),
        (None, None, "b", "|", None),
        (None, None, ",", "|", ["tamr_id", "first_name", "last_name", "ssn", "all_names"]),
        (None, None, ",", "|", ["tamr_id", "all_names", "ssn", "last_name", "first_name"]),
    ],
)
@mock_api()
def test_dataset_export_csv(
    buffer_size: Optional[int],
    nrows: Optional[int],
    csv_delimiter: str,
    flatten_delimiter: str,
    columns: List[str],
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with tempfile.TemporaryDirectory() as tempdir:
        filename = Path(tempdir) / f"test_export_csv_{timestamp}.csv"
        records_written = csv.from_dataset(
            dataset,
            filename,
            overwrite=True,
            buffer_size=buffer_size,
            nrows=nrows,
            csv_delimiter=csv_delimiter,
            flatten_delimiter=flatten_delimiter,
            columns=columns,
        )

        # Load raw export data and sort for comparison.
        # Clean up default handling of multi-values where a value is empty (e.g. "|Tuck") since the
        # toolbox does not export in this way (e.g. "Tuck")
        compare_to_df = pd.read_csv(
            io.StringIO(TEST_DATA), dtype="object", index_col="tamr_id"
        ).sort_index()
        compare_to_df["all_names"] = compare_to_df["all_names"].str.strip("|").str.split("|")

        test_df = pd.read_csv(
            filename, dtype="object", delimiter=csv_delimiter, index_col="tamr_id", quotechar='"'
        ).sort_index()
    test_df["all_names"] = test_df["all_names"].str.split(flatten_delimiter)

    # Sort columns of test data
    if columns is not None:
        # Drop tamr_id as it has been promoted to index
        columns.remove("tamr_id")
        compare_to_df = compare_to_df[columns]

    if nrows is None:
        assert test_df.equals(compare_to_df)
        assert records_written == len(compare_to_df)
    else:
        assert records_written == min(nrows, len(compare_to_df))
        assert len(test_df) == nrows
        # Check that subset of rows are present in comparison dataframe
        # Must convert Nulls to empty strings since None!=None
        # perform check first on the rows (axis=0) and then on the series
        # (note in later versions checking of pandas checking both axes separately to condense to
        # a single boolean can be done by passing simply axis=None, but performing in this way
        # supports earlier version as well)

        assert test_df.fillna("").isin(compare_to_df.fillna("")).all(axis=0).all()


@mock_api()
def test_dataset_export_csv_delim_error():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = Path(tempdir) / "test_taxonomy_invalid_delim.csv"

        with pytest.raises(ValueError):
            csv.from_dataset(dataset, filepath, csv_delimiter="|", flatten_delimiter="|")


@pytest.mark.parametrize("overwrite", [False])
@mock_api()
def test_dataset_overwrite_file(overwrite: bool):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = Path(tempdir) / "test_dataset_overwrite_{overwrite}.csv"

        f = open(filepath, "w")
        f.write("Temporary file")
        f.close()

        with pytest.raises(FileExistsError):
            csv.from_dataset(
                dataset, filepath, csv_delimiter=",", flatten_delimiter="|", overwrite=overwrite
            )


@pytest.mark.parametrize(
    "buffer_size, nrows, columns", [(None, None, None), (None, None, ["id", "ssn", "last_name"])],
)
@mock_api()
def test_dataset_export_csv_empty_dataset(
    buffer_size: Optional[int], nrows: Optional[int], columns: Optional[List[str]],
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    empty_dataset_id = CONFIG["datasets"]["people_0_records"]
    dataset = client.datasets.by_resource_id(empty_dataset_id)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with tempfile.TemporaryDirectory() as tempdir:
        filename = Path(tempdir) / f"test_export_csv_{timestamp}.csv"
        records_written = csv.from_dataset(
            dataset,
            filename,
            overwrite=True,
            buffer_size=buffer_size,
            nrows=nrows,
            columns=columns,
        )
        records_written = csv.from_dataset(
            dataset,
            filename,
            overwrite=True,
            buffer_size=buffer_size,
            nrows=nrows,
            columns=columns,
        )

        header_string = (
            EMPTY_TEST_DATA if columns is None else ",".join(f'"{col}"' for col in columns)
        )

        # Load raw export data and sort for comparison.
        compare_to_df = pd.read_csv(
            io.StringIO(header_string), dtype="object", index_col="id"
        ).sort_index()

        test_df = pd.read_csv(
            filename, dtype="object", delimiter=",", index_col="id", quotechar='"'
        ).sort_index()

    assert test_df.equals(compare_to_df)
    assert records_written == len(compare_to_df)


@mock_api()
def test_taxonomy_export_csv():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["minimal_categorization"]
    project = client.projects.by_resource_id(categorization_project_id)
    filepath = os.path.join(get_toolbox_root_dir(), "tests/data_io/temp_taxonomy.csv")
    records_written = csv.from_taxonomy(project, filepath, csv_delimiter=",")
    list_written = list(io.open(filepath))
    list_ref = TAXONOMY_DATA

    assert len(list_ref) == len(list_written)
    assert len(list_ref) == records_written

    for i in range(len(list_ref)):
        assert list_ref[i] == list_written[i]
    os.remove(filepath)


@mock_api()
def test_taxonomy_existing_file():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["minimal_categorization"]
    project = client.projects.by_resource_id(categorization_project_id)
    filepath = os.path.join(get_toolbox_root_dir(), "tests/data_io/temp_taxonomy2.csv")
    f = open(filepath, "w")
    f.write("Temporary file")
    f.close()

    with pytest.raises(FileExistsError):
        csv.from_taxonomy(project, filepath, overwrite=False)
    os.remove(filepath)


@mock_api()
def test_taxonomy_overwrite_file():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["minimal_categorization"]
    project = client.projects.by_resource_id(categorization_project_id)
    filepath = os.path.join(get_toolbox_root_dir(), "tests/data_io/temp_taxonomy3.csv")

    f = open(filepath, "w")
    f.write("Temporary file")
    f.close()

    records_written = csv.from_taxonomy(project, filepath, overwrite=True)
    list_ref = TAXONOMY_DATA
    list_written = list(io.open(filepath))

    assert records_written == len(list_ref)
    for i in range(len(list_ref)):
        assert list_ref[i] == list_written[i]
    os.remove(filepath)


@mock_api()
def test_taxonomy_check_invalid_file_path():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["minimal_categorization"]
    project = client.projects.by_resource_id(categorization_project_id)
    filepath = "/does_not_exist/temp_taxonomy.csv"

    with pytest.raises(IOError):
        csv.from_taxonomy(project, filepath)


@mock_api()
def test_taxonomy_check_invalid_project_type():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    master_project_id = CONFIG["projects"]["minimal_mastering"]
    project = client.projects.by_resource_id(master_project_id)
    filepath = os.path.join(get_toolbox_root_dir(), "tests/data_io/temp_taxonomy.csv")

    with pytest.raises(TypeError):
        csv.from_taxonomy(project, filepath, csv_delimiter=",", flatten_delimiter=",")


@mock_api()
def test_taxonomy_invalid_delimiters():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["minimal_categorization"]
    project = client.projects.by_resource_id(categorization_project_id)

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = Path(tempdir) / "tests/data_io/temp_taxonomy_invalid_delim.csv"

        with pytest.raises(ValueError):
            csv.from_taxonomy(project, filepath, csv_delimiter=",", flatten_delimiter=",")


@mock_api()
def test_taxonomy_missing():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    categorization_project_id = CONFIG["projects"]["categorization_no_taxonomy"]
    project = client.projects.by_resource_id(categorization_project_id)

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = Path(tempdir) / "tests/data_io/temp_taxonomy_missing.csv"

        with pytest.raises(RuntimeError):
            csv.from_taxonomy(project, filepath, csv_delimiter=",", flatten_delimiter="|")


@mock_api()
def test_dataset_bad_encoding():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    bad_encoding_dataset_id = CONFIG["datasets"]["bad_encoding"]
    dataset = client.datasets.by_resource_id(bad_encoding_dataset_id)

    filepath = os.path.join(get_toolbox_root_dir(), "tests/data_io/temp_bad_encoding.csv")
    csv.from_dataset(dataset, filepath)
    os.remove(filepath)


@pytest.mark.parametrize(
    "columns, column_name_dict",
    [
        (
            None,
            {"first_name": "new_first_name", "last_name": "family_name"},
        ),  # basic test renaming test
        (
            None,
            {"not_real_col": "full_name", "last_name": "family_name"},
        ),  # test with column name not present in the data
        (
            ["tamr_id", "all_names", "ssn", "last_name", "first_name"],
            {"last_name": "family_name", "ssn": "SS", "first_name": "given_name"},
        ),  # test with columns list -- columns order is preserved regardless of mapping dict order
    ],
)
@mock_api()
def test_dataset_renaming_csv_columns(
    columns: List[str], column_name_dict: Dict[str, str],
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with tempfile.TemporaryDirectory() as tempdir:
        filename = Path(tempdir) / f"test_export_csv_{timestamp}.csv"
        records_written = csv.from_dataset(
            dataset, filename, overwrite=True, columns=columns, column_name_dict=column_name_dict,
        )

        # Load raw export data and sort for comparison.
        # Clean up default handling of multi-values where a value is empty (e.g. "|Tuck") since the
        # toolbox does not export in this way (e.g. "Tuck")
        compare_to_df = pd.read_csv(
            io.StringIO(TEST_DATA), dtype="object", index_col="tamr_id"
        ).sort_index()
        compare_to_df["all_names"] = compare_to_df["all_names"].str.strip("|").str.split("|")

        # Sort columns of test data
        if columns is not None:
            columns.remove("tamr_id")
            compare_to_df = compare_to_df[columns]
        # CSV renaming should behave the same way as pandas dataframe renaming
        compare_to_df.rename(columns=column_name_dict, inplace=True)

        test_df = pd.read_csv(
            filename, dtype="object", delimiter=",", index_col="tamr_id", quotechar='"'
        ).sort_index()
    test_df["all_names"] = test_df["all_names"].str.split("|")

    assert records_written == len(compare_to_df)
    assert test_df.fillna("").isin(compare_to_df.fillna("")).all(axis=0).all()


@mock_api()
def test_dataset_invalid_renaming_map(
    columns=["tamr_id", "all_names", "ssn", "last_name", "first_name"],
    column_name_dict={"all_names": "last_name"},
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["minimal_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    with tempfile.TemporaryDirectory() as tempdir:
        filename = Path(tempdir) / f"test_export_csv_{timestamp}.csv"

        # ValueError raised by renaming that would yield duplicate columns
        with pytest.raises(ValueError):
            csv.from_dataset(
                dataset,
                filename,
                overwrite=True,
                columns=columns,
                column_name_dict=column_name_dict,
            )


@mock_api()
def test_dataset_not_streamable():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    sm_dataset_id = CONFIG["datasets"]["broken_schema_mapping_unified_dataset"]
    dataset = client.datasets.by_resource_id(sm_dataset_id)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = Path(tempdir) / f"test_dataset_not_streamable_{timestamp}.csv"

        # ValueError raised by renaming that would yield duplicate columns
        with pytest.raises(RuntimeError):
            csv.from_dataset(dataset, filepath, allow_dataset_refresh=False)
