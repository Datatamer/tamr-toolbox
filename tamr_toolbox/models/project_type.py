"""Enum representing project types in Tamr"""
from enum import Enum


class ProjectType(Enum):
    """A dataclass representing the project type codes in Tamr

    Args:
        SCHEMA_MAPPING_RECOMMENDATIONS: Schema Mapping Project Type
        DEDUP: Mastering Project Type
        GOLDEN_RECORDS: Golden Record Project Type
        CATEGORIZATION: Categorization Project Type
    """

    SCHEMA_MAPPING_RECOMMENDATIONS = "SCHEMA_MAPPING_RECOMMENDATIONS"
    DEDUP = "DEDUP"
    GOLDEN_RECORDS = "GOLDEN_RECORDS"
    CATEGORIZATION = "CATEGORIZATION"
