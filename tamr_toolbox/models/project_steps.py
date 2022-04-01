# define Enum classes for project steps
from enum import Enum


class CategorizationSteps(Enum):
    """
    Enum class for the steps of a classification project
    """

    UPDATE_UNIFIED_DATASET = "updated_unified_dataset"
    APPLY_FEEDBACK = "apply_feedback"
    UPDATE_RESULTS_ONLY = "update_results_only"


class SchemaMappingSteps(Enum):
    """
    Enum class for the steps of a schema mapping project
    """

    UPDATE_UNIFIED_DATASET = "updated_unified_dataset"


class MasteringSteps(Enum):
    """
    Enum class for the steps of a mastering project
    """

    UPDATE_UNIFIED_DATASET = "update_unified_dataset"
    ESTIMATE_PAIR_COUNTS = "estimate_pair_counts"
    GENERATE_PAIRS = "generate_pairs"
    APPLY_FEEDBACK = "apply_feedback"
    UPDATE_HIGH_IMPACT_PAIRS = "update_high_impact_pairs"
    UPDATE_CLUSTERS = "update_clusters"
    APPLY_FEEDBACK_AND_UPDATE_RESULTS = "apply_feedback_and_update_results"
    UPDATE_RESULTS_ONLY = "update_results_only"
    PUBLISH_CLUSTERS = "publish_clusters"


class GoldenRecordsSteps(Enum):
    """
    Enum class for the steps of a golden records project
    """

    PROFILE_GOLDEN_RECORDS = "profile_golden_records"
    UPDATE_GOLDEN_RECORDS = "update_golden_records"
    PUBLISH_GOLDEN_RECORDS = "publish_golden_records"
