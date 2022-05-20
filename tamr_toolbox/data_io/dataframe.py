"""Tasks related to moving data in or out of Tamr using pandas.DataFrames"""
import os
import logging

from typing import Optional, List, Callable, Any, Iterable, Tuple
from collections import OrderedDict, defaultdict
from functools import partial

from tamr_unify_client.dataset.resource import Dataset

from tamr_toolbox.data_io import common
from tamr_toolbox.models.validation_check import ValidationCheck
from tamr_toolbox.utils import operation


LOGGER = logging.getLogger(__name__)

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    import pandas


def _flatten_list_series(
    series: "pandas.Series",
    *,
    delimiter: str = "|",
    force: bool = False,
    columns: Optional[List] = None,
) -> "pandas.Series":
    """
    Flattens list types to strings.  See ``_flatten_list`` for details.
    Operates on a pandas Series, and is intended to be applied using `apply` to a pandas DataFrame.

    Args:
        series: pandas Series of values to flatten
        delimiter: string to use as delimiter for concatenating lists
        force: if True, will force non-string inner types to string
        columns: optional, list of columns to flatten

    Returns: Series of flattened values
    """
    func = partial(common._flatten_list, delimiter=delimiter, force=force)
    # if we haven't specified which columns, or this column is in the list, apply the function
    if columns is None or series.name in columns:
        return series.apply(func)
    # otherwise do nothing
    else:
        return series


def flatten(
    df: "pandas.DataFrame",
    *,
    delimiter: str = "|",
    columns: Optional[List[str]] = None,
    force: bool = False,
) -> "pandas.DataFrame":
    """
    Converts DataFrame columns with list type to strings
    and returns a copy of the DataFrame with this change.
    Tamr often produces datasets with columns of list type,
    which often are more easily consumed as single-valued columns.

    Args:
        df: DataFrame from Tamr dataset
        delimiter: string to use as delimiter for concatenating lists
        columns: optional, list of columns to flatten
        force: if True, will force non-string inner types to string

    Returns:
        flattened DataFrame
    """
    # apply a series-wise function that can operate on specific columns of the dataframe
    series_func = partial(_flatten_list_series, delimiter=delimiter, force=force, columns=columns)
    return df.apply(series_func)


def from_dataset(
    dataset: Dataset,
    *,
    columns: Optional[List[str]] = None,
    flatten_delimiter: Optional[str] = None,
    flatten_columns: Optional[List[str]] = None,
    force_flatten: bool = False,
    nrows: Optional[int] = None,
    allow_dataset_refresh: bool = False,
) -> "pandas.DataFrame":
    """
    Creates a DataFrame from a Tamr Dataset

    Args:
        dataset: Tamr Dataset object
        columns: optional, ordered list of columns to keep
        flatten_delimiter: if set, flatten list types to strings by concatenating with this
            delimiter
        flatten_columns: optional, list of columns to flatten
        force_flatten:  if False, arrays with inner types other than string will not be flattened.
            if True, will force all inner types to strings when flattening values.
            if True, `flatten_delimiter` must be specified.
        nrows: number of rows to read. default None will read all rows
        allow_dataset_refresh: if True, allows running a job to refresh dataset to make streamable

    Returns:
        DataFrame

    Raises:
        ValueError: if `columns` or `flatten_columns` contain columns that are not present in
            `dataset`
    """
    # Validate arguments
    if force_flatten and flatten_delimiter is None:
        raise ValueError("`force_flatten` is True but no `flatten_delimiter` is specified.")

    # This function requires pandas, an optional dependency
    import pandas

    LOGGER.info(
        f"Streaming records to DataFrame for dataset {dataset.name} (id={dataset.resource_id})."
    )
    dataset_attrs = [attr for attr in dataset.attributes]
    attr_names = [attr.name for attr in dataset_attrs]
    # check that specified columns exist
    if columns is not None:
        common._check_columns_subset(
            input_list=columns, reference_list=attr_names, raise_error=True
        )
    # checks on columns to flatten
    if flatten_delimiter is not None:
        if flatten_columns is None:
            flatten_columns = list(attr_names)
        else:
            # check that specified columns exist
            common._check_columns_subset(
                input_list=flatten_columns, reference_list=attr_names, raise_error=True
            )
        # check types of flatten_columns
        for attr in dataset_attrs:
            if attr.name not in flatten_columns:
                continue
            attr_type = attr.spec().to_dict()["type"]
            if attr_type["baseType"] == "ARRAY" and attr_type["innerType"]["baseType"] != "STRING":
                if force_flatten:
                    LOGGER.info(
                        f"Will force attribute to string: {attr.name}, with type: {attr_type}"
                    )
                else:
                    LOGGER.warning(
                        f"Will not flatten attribute: {attr.name}, with type: {attr_type}"
                    )
                    flatten_columns.remove(attr.name)

    if not dataset.status().is_streamable:
        if allow_dataset_refresh:
            LOGGER.info(f"Refreshing dataset {dataset.name} to make streamable.")
            op = dataset.refresh()
            operation.enforce_success(op)
        else:
            message = (
                f"Dataset {dataset.name} is not streamable. Refresh it first, or run"
                f" with allow_dataset_refresh=True"
            )
            LOGGER.error(message)
            raise RuntimeError(message)

    # if flattening, set the function to apply to records as _flatten_list
    # otherwise set as _identity
    func = None
    if flatten_delimiter is not None:
        func = partial(common._flatten_list, delimiter=flatten_delimiter, force=force_flatten)
    df = pandas.DataFrame.from_records(
        common._yield_records(
            dataset, func=func, columns=columns, flatten_columns=flatten_columns
        ),
        columns=columns,
        nrows=nrows,
    )
    return df


def _count_null(s: "pandas.Series") -> int:
    """
    Returns the number of null values in a pandas Series

    Args:
        s: pandas Series
    Returns:
        int
    """
    return s.isna().sum()


def profile(df: "pandas.DataFrame") -> "pandas.DataFrame":
    """
    Computes profile statistics from an input DataFrame,
    and returns statistics in another DataFrame.
    Intended to be used for validation checks on a DataFrame before
    upserting records to a Tamr Dataset

    Args:
        df: DataFrame

    Returns:
        DataFrame with profile statistics
    """
    # compute per-attribute metrics
    # attr_metrics: aggregations that return a Series indexed by the attribute names
    # attr_metric_names: aggregations that return a Series indexed by the attribute names
    attr_agg_functions = OrderedDict()

    # number of unique values
    attr_agg_functions["DistinctValueCount"] = "nunique"

    # number of null values
    attr_agg_functions["EmptyValueCount"] = _count_null

    # aggregate to compute per-attribute metrics
    df_profile = df.agg(list(attr_agg_functions.values()), axis=0).T
    # rename columns
    df_profile.columns = attr_agg_functions.keys()

    # add summary level metrics
    df_profile["RecordCount"] = df.shape[0]

    return df_profile


def _check_present_columns(
    df: "pandas.DataFrame", *, require_present_columns: Optional[List[str]] = None
) -> ValidationCheck:
    """
    Checks that a specified list of columns in a DataFrame are all present

    Args:
        df: DataFrame
        require_present_columns: list of columns that are checked to be present

    Returns:
        ValidationCheck object, with bool for whether check passed and dict of failing columns
    """
    failed_checks_dict = defaultdict(list)

    if require_present_columns is None:
        require_present_columns = []

    # check if columns are present
    for col in require_present_columns:
        if col not in df.columns:
            LOGGER.warning(f"column {col} is not present")
            failed_checks_dict["failed_present_columns"].append(col)

    passed = len(failed_checks_dict) == 0
    return ValidationCheck(passed, failed_checks_dict)


def _check_unique_columns(
    df_profile: "pandas.DataFrame", *, require_unique_columns: Optional[List[str]] = None
) -> ValidationCheck:
    """
    Checks that a specified list of columns in a DataFrame have all unique values

    Args:
        df_profile: DataFrame containing profile information
        require_unique_columns: list of columns that are checked to have all unique values,
            e.g. a primary key column

    Returns:
        ValidationCheck object, with bool for whether check passed and dict of failing columns
    """
    failed_checks_dict = defaultdict(list)

    if require_unique_columns is None:
        require_unique_columns = []

    # check for unique columns
    for col in require_unique_columns:
        num_unique = df_profile.loc[col, "DistinctValueCount"]
        num_records = df_profile.loc[col, "RecordCount"]
        if not num_unique == num_records:
            LOGGER.warning(
                f"column {col} has only {num_unique} unique values out of {num_records} records"
            )
            failed_checks_dict["failed_unique_columns"].append(col)

    passed = len(failed_checks_dict) == 0
    return ValidationCheck(passed, failed_checks_dict)


def _check_nonnull_columns(
    df_profile: "pandas.DataFrame", *, require_nonnull_columns: Optional[List[str]] = None
) -> ValidationCheck:
    """
    Checks that a specified list of columns in a DataFrame have all non-null values

    Args:
        df_profile: DataFrame containing profile information
        require_nonnull_columns: list of columns that are checked to have all non-null values

    Returns:
        ValidationCheck object, with bool for whether check passed and dict of failing columns
    """
    failed_checks_dict = defaultdict(list)

    if require_nonnull_columns is None:
        require_nonnull_columns = []

    # check for nonnull columns
    for col in require_nonnull_columns:
        num_null = df_profile.loc[col, "EmptyValueCount"]
        if num_null > 0:
            LOGGER.warning(f"column {col} has {num_null} null values")
            failed_checks_dict["failed_nonnull_columns"].append(col)

    passed = len(failed_checks_dict) == 0
    return ValidationCheck(passed, failed_checks_dict)


def _check_custom(
    df: "pandas.DataFrame",
    columns_to_check: Optional[List[str]],
    check_function: Callable[[Any], bool],
) -> ValidationCheck:
    """
    Args:
        df: Dataframe
        columns_to_check: columns on which check_function will be applied
        check_function: function applied on columns_to_check

    Returns:
        ValidationCheck object, with bool for whether all checks passed and dict of failing columns
    """
    failed_checks_dict = defaultdict(list)

    df1 = df[columns_to_check].applymap(check_function)
    for col in columns_to_check:
        if not df1[col].all():
            LOGGER.warning(f"column {col} failed custom check {check_function.__name__}")
            failed_checks_dict[f"failed custom check {check_function.__name__}"].append(col)

    passed = len(failed_checks_dict) == 0
    return ValidationCheck(passed, failed_checks_dict)


def validate(
    df: "pandas.DataFrame",
    *,
    raise_error: bool = True,
    require_present_columns: Optional[List[str]] = None,
    require_unique_columns: Optional[List[str]] = None,
    require_nonnull_columns: Optional[List[str]] = None,
    custom_checks: Iterable[Tuple[Callable[[Any], bool], List[str]]] = tuple(),
) -> ValidationCheck:
    """
    Performs validation checks on a DataFrame.
    Returns a dict of columns that fail each check, and optionally returns an error.
    Intended to be used on a DataFrame prior to upserting records into a Tamr dataset.

    Args:
        df: DataFrame
        raise_error: if True, will raise a ValueError on failing checks.  if False,
            will print Warning and return a dict
        require_present_columns: list of columns that are checked to be present
        require_unique_columns: list of columns that are checked to have all unique values,
            e.g. a primary key column
        require_nonnull_columns: list of columns that are checked to have all non-null values
        custom_checks: collection of tuples each containing a custom function and list of columns,
            on which to apply it


    Returns:
        ValidationCheck object, with bool for whether all checks passed and dict of failing columns

    Raises:
        ValueError: if `raise_error` is set True, and any checks fail
    """
    failed_checks_dict = defaultdict(list)

    if require_present_columns is None:
        require_present_columns = []
    if require_unique_columns is None:
        require_unique_columns = []
    if require_nonnull_columns is None:
        require_nonnull_columns = []

    # compute profile stats
    df_profile = profile(df)

    # check for present columns
    failed_checks_dict.update(
        _check_present_columns(df, require_present_columns=require_present_columns).details
    )

    # check for unique columns
    failed_checks_dict.update(
        _check_unique_columns(df_profile, require_unique_columns=require_unique_columns).details
    )

    # check for nonnull columns
    failed_checks_dict.update(
        _check_nonnull_columns(df_profile, require_nonnull_columns=require_nonnull_columns).details
    )

    for custom_check in custom_checks:
        failed_checks_dict.update(
            _check_custom(
                df, check_function=custom_check[0], columns_to_check=custom_check[1],
            ).details
        )

    failed_checks_dict = dict(failed_checks_dict)
    passed = len(failed_checks_dict) == 0
    if not passed and raise_error:
        raise (
            ValueError(
                f"DataFrame validation failed.  failed columns for each check: "
                f"{failed_checks_dict}"
            )
        )
    return ValidationCheck(passed, failed_checks_dict)
