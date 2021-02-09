"""Dataclass for validation check information"""
from dataclasses import dataclass

from tamr_toolbox.models.data_type import JsonDict


@dataclass
class ValidationCheck:
    """A dataclass for exchanging the results of validation checks.

    Args:
        passed: A boolean indicating True if the validation check passed
        details: A JSON object containing more information about the results of the validation
            check
    """

    passed: bool  # whether the check passed
    details: JsonDict  # detailed results for each check
