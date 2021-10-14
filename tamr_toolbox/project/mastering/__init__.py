"""Tasks related to Tamr Mastering projects"""
from . import jobs
from .._common import schema, transformations

__all__ = ["jobs", "schema", "transformations"]

# define Enum classes for project steps
from enum import Enum


class Steps(Enum):
    """
    Enum class for the steps of a mastering project
    """

    UPDATE_UNIFIED_DATASET = "update_unified_dataset"
    ESTIMATE_PAIR_COUNTS = "estimate_pair_counts"
    GENERATE_PAIRS = "generate_pairs"
    TRAIN_MASTERING_MODEL = "train_mastering_model"
    UPDATE_HIGH_IMPACT_PAIRS = "update_high_impact_pairs"
    UPDATE_CLUSTERS = "update_clusters"
    APPLY_FEEDBACK_AND_UPDATE_RESULTS = "apply_feedback_and_update_results"
    UPDATE_RESULTS_ONLY = "update_results_only"
    PUBLISH_CLUSTERS = "publish_clusters"
