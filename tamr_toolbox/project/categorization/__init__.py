"""Tasks related to Tamr Categorization projects"""
from . import jobs
from . import metrics
from .._common import schema, transformations

__all__ = ["jobs", "schema", "transformations", "metrics"]

# define Enum classes for project steps
from enum import Enum


class Steps(Enum):
    """
    Enum class for the steps of a classification project
    """

    UPDATE_UNIFIED_DATASET = "updated_unified_dataset"
    APPLY_FEEDBACK_AND_UPDATE_RESULTS = "apply_feedback_and_update_results"
    UPDATE_RESULTS_ONLY = "update_results_only"
