"""Tasks common to moving data in and out of Tamr"""
from typing import Optional, List, Any, Iterable, Callable, Dict
import logging

from tamr_unify_client.dataset.resource import Dataset

LOGGER = logging.getLogger(__name__)


def _flatten_list(val: Any, *, delimiter: str = "|", force: bool = False) -> Any:
    """
    Flattens list types to strings.
    By default, only list[string] is flattened to a single string.
    The `force` parameter allows flattening of other inner types like numerics to a string.
    A list of length 1 becomes a string.
    A list with length longer than 1 has its elements concatenated with the specified `delimiter`.
    Non-list types are passed through directly.
    Intended to be applied using `applymap` to a DataFrame created from a Tamr Dataset,
    or to a pandas Series using `apply`.

    Args:
        val: value to convert
        delimiter: string to use as delimiter for concatenating lists
        force: if True, will force non-string inner types to string

    Returns:
        string or original value
    """
    if isinstance(val, list):
        if force:
            # convert inner values to strings.  f-strings are faster than str()
            return delimiter.join(f"{i}" for i in filter(None, val))
        else:
            return delimiter.join(filter(None, val))
    else:
        return val


def _yield_records(
    dataset: Dataset,
    *,
    func: Optional[Callable] = None,
    columns: Optional[List] = None,
    flatten_columns: Optional[List] = None,
) -> Iterable:
    """
    Generator function for records from a Tamr Dataset.
    Can optionally apply a flattening function to some or all columns,
    and can optionally keep only a subset of columns from the original records.

    Args:
        dataset: Tamr Dataset
        func: optional, callable function to transform records
        columns: optional, list of columns to keep
        flatten_columns: optional, list of columns to flatten

    Returns: iterable over Dataset records

    Raises:
        ValueError: if `flatten_columns` is not None but `func` is None since it
            won't know how to flatten them.
    """

    if flatten_columns is not None and func is None:
        message = (
            f"Columns specified for flatting ('flatten_columns'={flatten_columns}), "
            f"but no flattening function provided for parameter 'func'"
        )
        LOGGER.error(message)
        raise ValueError(message)

    checked_columns = False
    for record in dataset.records():
        if not checked_columns:
            if columns is not None:
                _check_columns_subset(
                    input_list=columns, reference_list=record.keys(), raise_error=True
                )
            if flatten_columns is not None:
                _check_columns_subset(
                    input_list=flatten_columns, reference_list=record.keys(), raise_error=True
                )
            checked_columns = True

        # Set flatten_columns to all if unspecified
        if func is not None and flatten_columns is None:
            flatten_columns = record.keys()

        output = {}
        for k, v in record.items():
            if columns is not None and k not in columns:
                # remove the column by skipping
                continue

            do_flatten_column = flatten_columns is not None and k in flatten_columns
            if do_flatten_column and func is not None:
                output[k] = func(v)
            else:
                output[k] = v
        yield output


def _check_columns_subset(
    *, input_list: List, reference_list: List, raise_error: bool = True
) -> bool:
    """
    Checks if `input_list` is a subset of `reference_list`.
    Returns a bool, or if `raise_error` is True, raises a ValueError on a failed check.

    Args:
        input_list: list to perform check for
        reference_list: reference list to check against for membership
        raise_error: if True, will raise a ValueError on failed check instead of returning

    Returns: bool

    Raises:
        ValueError: if `input_list` is not a subset of `reference_list`, and `raise_error` is True
    """
    input_set = set(input_list)
    reference_set = set(reference_list)
    if not input_set.issubset(reference_set):
        if raise_error:
            message = f"column(s) {input_set.difference(reference_set)} not found"
            LOGGER.error(message)
            raise ValueError(message)
        else:
            return False
    return True


def _get_column_mapping_dict(
    *,
    dataset_attribute_names: Iterable[str],
    column_name_dict: Dict[str, str] = None,
    columns: List[str] = None,
) -> Dict[str, str]:
    """
    Generate a dictionary to support column re-naming in CSV output.

    Args:
        dataset_attribute_names: Names of Tamr Dataset columns
        column_name_dict: Optional, Dictionary containing renaming information in the form
            {<Tamr dataset attribute name>: <new name>}. If None, preserve original names
        columns: Optional, List of columns to include in output. If None, use all columns
    Returns:
        Dictionary mapping Tamr Dataset attribute names to new column names
    Raises:
        ValueError: if column renaming would yield duplicate column names
    """

    if columns is None:
        columns = dataset_attribute_names

    if column_name_dict is not None:
        # Warn if the supplied column mapping has keys which don't appear in the dataset
        is_subset = _check_columns_subset(
            input_list=column_name_dict.keys(),
            reference_list=dataset_attribute_names,
            raise_error=False,
        )

        if not is_subset:
            message = (
                f"Some key column(s) in the column mapping {column_name_dict}"
                "do not appear in specified Tamr dataset."
            )
            LOGGER.warning(message)

        # Make mapping dict, mapping columns to themselves if they're not in the column_name_dict
        # Dict is generated by iterating through columns list so that order of columns is preserved
        full_column_name_dict = {col: column_name_dict.get(col, col) for col in columns}

        # Check that renaming won't generate duplicate columns in the output
        if len(set(full_column_name_dict.values())) < len(full_column_name_dict):
            message = "Column renaming would generate duplicate column names in output."
            LOGGER.error(message)
            raise ValueError(message)

    else:  # If there's no column renaming dictionary supplied, just map columns to themselves
        full_column_name_dict = dict(zip(columns, columns))

    return full_column_name_dict
