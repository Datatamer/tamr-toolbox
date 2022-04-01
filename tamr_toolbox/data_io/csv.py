"""Tasks related to moving data in or out of Tamr using delimited files"""
from typing import Optional, List, Union, Dict
from functools import partial
from pathlib import Path

import csv
import logging
import os
import requests

from tamr_unify_client.dataset.resource import Dataset
from tamr_unify_client.project.resource import Project

from tamr_toolbox.utils import operation
from tamr_toolbox.data_io import common

LOGGER = logging.getLogger(__name__)


def from_dataset(
    dataset: Dataset,
    export_file_path: Union[Path, str],
    *,
    csv_delimiter: str = ",",
    columns: Optional[List[str]] = None,
    column_name_dict: Optional[Dict[str, str]] = None,
    flatten_delimiter: str = "|",
    quote_character: str = '"',
    quoting: int = csv.QUOTE_MINIMAL,
    na_value: str = "NaN",
    nrows: Optional[int] = None,
    allow_dataset_refresh: bool = False,
    buffer_size: int = 10000,
    overwrite: bool = False,
    encoding: str = "utf-8",
) -> int:
    """
    Export a Tamr Dataset to a csv file. Records are streamed to disk and written according to a
    given buffer size. As a result this is more memory efficient than first reading to a
    pandas.DataFrame and writing to CSV.

    Args:
        dataset: Tamr Dataset object
        export_file_path: Path to the csv file where the dataset will be saved
        csv_delimiter: Delimiter of the csv file
        columns: Optional, Ordered list of columns to write. If None, write all columns in
            arbitrary order.
        column_name_dict: Optional, Dictionary in the format {<Tamr dataset column name> : <new csv
            column name>}, used to rename some or all columns in the output file.
        flatten_delimiter: Flatten list types to strings by concatenating with this delimiter
        quote_character: Character used to escape value for csv delimiter when it appears in the
            value.
        quoting: The escape strategy to use according to the Python csv writer.
            See https://docs.python.org/2/library/csv.html#csv.QUOTE_MINIMAL
        na_value: Value to write that represents empty or missing data.
            See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
            for the na_values supported by default in pandas.read_csv
        nrows: Optional, Number of rows to write. If None, then write all rows.
        allow_dataset_refresh: If True, allows running a job to refresh dataset to make streamable.
            Otherwise a RuntimeError will be thrown if the dataset is unstreamable.
        buffer_size: Number of records to store in memory before writing to disk
        overwrite: if True and export_file_name already exists, overwrite the file.
            Otherwise throw an error
        encoding: The encoding to use in the written file.
            See https://docs.python.org/3/library/functions.html#open

    Returns:
        The total number of records written

    Raises:
        FileExistsError: if the csv file to which the dataset is to be streamed exists
            and `overwrite` is False
        RuntimeError: if `dataset` is not streamable and `allow_dataset_refresh` is False
        ValueError: if `columns` or `flatten_columns` contain columns that are not
            present in `dataset`, or if column renaming would yield duplicate column names
    """
    LOGGER.info(
        f"Streaming records to csv file {export_file_path} from dataset {dataset.name} "
        f"(id={dataset.resource_id})."
    )

    if os.path.exists(export_file_path):
        if not overwrite:
            message = (
                f"CSV file {export_file_path} already exists. "
                f"(Set 'overwrite' flag to True if you wish to overwrite)"
            )
            LOGGER.error(message)
            raise FileExistsError(message)
        else:
            LOGGER.warning(f"CSV file {export_file_path} already exists and will be overwritten")

    if csv_delimiter == flatten_delimiter:
        message = (
            f"The CSV delimiter '{csv_delimiter}' cannot be identical to the list "
            f"flattening delimiter '{flatten_delimiter}'"
        )
        LOGGER.error(message)
        raise ValueError(message)

    attribute_names = [attr.name for attr in dataset.attributes]

    # check that specified columns exist
    if columns is not None:
        common._check_columns_subset(
            input_list=columns, reference_list=attribute_names, raise_error=True
        )

    if not dataset.status().is_streamable:
        if allow_dataset_refresh:
            LOGGER.info(f"Refreshing dataset {dataset.name} to make streamable.")
            op = dataset.refresh()
            operation.enforce_success(op)
        else:
            message = (
                f"Dataset {dataset.name} is not streamable. Refresh it first, or "
                f"run with allow_dataset_refresh=True"
            )
            LOGGER.error(message)
            raise RuntimeError(message)

    func = partial(common._flatten_list, delimiter=flatten_delimiter, force=True)

    full_column_name_dict = common._get_column_mapping_dict(
        dataset_attribute_names=attribute_names,
        column_name_dict=column_name_dict,
        columns=columns,
    )

    # Open CSV file and use newline='' as recommended by
    # https://docs.python.org/3/library/csv.html#csv.writer
    with open(export_file_path, "w", newline="", encoding=encoding) as csv_file:
        csv_writer = csv.writer(
            csv_file, delimiter=csv_delimiter, quotechar=quote_character, quoting=quoting,
        )
        buffer = []
        header = None
        # Set record number to -1 in case the dataset streamed has no records
        record_number = -1

        for record_number, record in enumerate(
            common._yield_records(dataset, func=func, columns=columns)
        ):
            # Obtain and write the header information only on the first pass
            if header is None:
                header = full_column_name_dict.values()
                csv_writer.writerow(header)

            # Replace empty values with a specific null value
            # This also allows nulls to be treated differently from empty strings
            record = [
                na_value if record[k] is None else record[k] for k in full_column_name_dict.keys()
            ]
            buffer.append(record)

            at_max_buffer = buffer_size is not None and (len(buffer) >= buffer_size)
            at_max_rows = nrows is not None and record_number >= nrows - 1
            if at_max_buffer or at_max_rows:
                csv_writer.writerows(buffer)
                LOGGER.debug(f"Written dataset {dataset.name} up to record {record_number+1}")
                buffer = []
                if at_max_rows:
                    break

        # Write anything remaining
        # This will occur whenever the buffer is non-zero and the number of records
        # is not exactly divisible by the buffer number
        # For example, writing a dataset with 1100 records using a buffer size of 500
        # will write in 3 chunks: 2 x 500 above and the remaining 100 handled here
        if len(buffer) != 0:
            LOGGER.debug(f"Written dataset {dataset.name} up to record {record_number + 1}")
            csv_writer.writerows(buffer)

        if record_number == -1:
            # If record number is -1 then no records were streamed, possibly because the dataset
            # has no records. We therefore want to simply save the headers
            if columns is not None:
                csv_writer.writerow(columns)
            else:
                csv_writer.writerow(attribute_names)

    records_written = record_number + 1

    LOGGER.info(
        f"Wrote {records_written} from dataset {dataset.name} (id={dataset.resource_id}) "
        f"to {export_file_path}"
    )

    return records_written


def from_taxonomy(
    project: Project,
    export_file_path: Union[Path, str],
    *,
    csv_delimiter: str = ",",
    flatten_delimiter: str = "|",
    quote_character: str = '"',
    quoting: int = csv.QUOTE_MINIMAL,
    overwrite: bool = False,
    encoding: str = "utf-8",
) -> int:
    """
    Export a Tamr taxonomy to a csv file. Records are streamed to disk and written according to a
    given buffer size.

    Args:
        project: Tamr Project object
        export_file_path: Path to the csv file where the dataset will be saved
        csv_delimiter: Delimiter of the csv file
        flatten_delimiter: Flatten list types to strings by concatenating with this delimiter
        quote_character: Character used to escape value for csv delimiter when it appears in the
            value.
        quoting: The escape strategy to use according to the Python csv writer.
            See https://docs.python.org/2/library/csv.html#csv.QUOTE_MINIMAL
        overwrite: if True and export_file_name already exists, overwrite the file.
            Otherwise throw an error
        encoding: The encoding to use in the written file.
            See https://docs.python.org/3/library/functions.html#open

    Returns:
        The total number of records written

    Raises:
        FileExistsError: if `export_file_path` exists and `overwrite` is set to False
        IOError: if the specified filepath does not exist or cannot be accessed
        RuntimeError: if the classification project is not yet associated with a taxonomy or
                      taxonomy cannot be written to a csv file
        TypeError: if the project type is not classification
        ValueError: if `columns` and `flatten_columns` are identical values
    """
    LOGGER.info(
        f"Streaming taxonomy to csv file {export_file_path} from project {project.name} "
        f"(project id={project.resource_id})."
    )

    try:
        project = project.as_categorization()
    except TypeError:
        not_categorization_error = f"Project {project.name} is not a classification project."
        LOGGER.error(not_categorization_error)
        raise TypeError(not_categorization_error)

    if os.path.exists(export_file_path):
        if not overwrite:
            message = (
                f"CSV file {export_file_path} already exists. "
                f"(Set 'overwrite' flag to True if you wish to overwrite)"
            )
            LOGGER.error(message)
            raise FileExistsError(message)
        else:
            LOGGER.warning(f"CSV file {export_file_path} already exists and will be overwritten")

    if csv_delimiter == flatten_delimiter:
        message = (
            f"The CSV delimiter '{csv_delimiter}' cannot be identical to the list "
            f"flattening delimiter '{flatten_delimiter}'"
        )
        LOGGER.error(message)
        raise ValueError(message)

    try:
        taxonomy = project.as_categorization().taxonomy()
    except requests.exceptions.RequestException:
        no_taxonomy_error = f"Project {project.name} is not associated with any taxonomy yet."
        LOGGER.error(no_taxonomy_error)
        raise RuntimeError(no_taxonomy_error)

    # obtain categories and store in a list
    categories = taxonomy.categories()
    taxonomy_list = []
    for category in categories:
        taxonomy_list.append(category.path)

    # sort the categories
    taxonomy_list.sort()

    # Open CSV file and use newline='' as recommended by
    # https://docs.python.org/3/library/csv.html#csv.writer

    try:
        f = open(export_file_path, "w", newline="", encoding=encoding)
    except (FileNotFoundError, IOError, PermissionError):
        cannot_open_error = f"File path {export_file_path} could not be opened for writing."
        LOGGER.error(cannot_open_error)
        raise IOError(cannot_open_error)
    else:
        try:
            csv_writer = csv.writer(
                f, delimiter=csv_delimiter, quotechar=quote_character, quoting=quoting,
            )
            csv_writer.writerows(taxonomy_list)
        except csv.Error as e:
            general_error = (
                "Encountered an error while writing taxonomy categories to "
                f"{export_file_path}: {e}"
            )
            f.close()
            LOGGER.error(general_error)
            raise RuntimeError(general_error)
        finally:
            f.close()

    records_written = len(taxonomy_list)
    LOGGER.info(
        f"Wrote {records_written} categories from {project.name} taxonomy (project id"
        f"={project.resource_id}) to {export_file_path}"
    )
    return records_written
