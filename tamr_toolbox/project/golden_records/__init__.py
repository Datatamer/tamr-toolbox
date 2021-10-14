"""Tasks related to Tamr Golden Records projects"""
from . import jobs

__all__ = ["jobs"]

# define Enum classes for project steps
from enum import Enum


class Steps(Enum):
    """
    Enum class for the steps of a golden records project
    """

    PROFILE_GOLDEN_RECORDS = "profile_golden_records"
    UPDATE_GOLDEN_RECORDS = "update_golden_records"
    PUBLISH_GOLDEN_RECORDS = "publish_golden_records"
