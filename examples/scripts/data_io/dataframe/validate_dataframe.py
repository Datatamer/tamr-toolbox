from tamr_toolbox.models.validation_check import ValidationCheck
import pandas as pd
from tamr_toolbox.data_io import dataframe


def main() -> ValidationCheck:
    """returns columns which failed checks
    Returns: ValidationCheck object consisting of Pass/Fail and dict of failed columns
    """

    def ensure_not_2(value):
        if value == 2:
            return False
        else:
            return True

    def ensure_not_3(value):
        if value == 3:
            return False
        else:
            return True

    df1 = pd.DataFrame(
        {"a": [1, 1, 1, 1], "b": [1, 1, 2, 2], "c": [2, 2, 2, 2], "d": [3, 3, 3, 3]}
    )

    failed_checks_dict = dataframe.validate(
        df1,
        require_present_columns=["a", "b"],
        require_unique_columns=["d"],
        require_nonnull_columns=["b", "c"],
        custom_check=((ensure_not_2, ["a", "b"]), (ensure_not_3, ["a", "d"])),
    )
    return failed_checks_dict


if __name__ == "__main__":
    main()
