""" Project artifacts data classes """
from dataclasses import dataclass


@dataclass()
class SchemaMappingArtifacts:
    """A dataclass representing artifact codes for Schema Mapping projects in Tamr

    Args:
        UNIFIED_ATTRIBUTES: artifact name for a schema mapping project
        TRANSFORMATIONS: artifact name for a schema mapping project
        SMR_MODEL: artifact name for a schema mapping project
        RECORD_COMMENTS: artifact name for a schema mapping project
    """

    # Schema Mapping artifacts
    UNIFIED_ATTRIBUTES: str = "UNIFIED_ATTRIBUTES"
    TRANSFORMATIONS: str = "TRANSFORMATIONS"
    SMR_MODEL: str = "SMR_MODEL"
    RECORD_COMMENTS: str = "RECORD_COMMENTS"


@dataclass()
class MasteringArtifacts:
    """A dataclass representing artifact codes for Mastering projects in Tamr

    Args:
        UNIFIED_ATTRIBUTES: artifact name for a Mastering project
        TRANSFORMATIONS: artifact name for a Mastering project
        SMR_MODEL: artifact name for a Mastering project
        RECORD_COMMENTS: artifact name for a Mastering project

        MASTERING_CONFIGURATION: artifact name for a Mastering project
        USER_DEFINED_SIGNALS: artifact name for a Mastering project
        MASTERING_FUNCTIONS: artifact name for a Mastering project
        RECORD_PAIR_COMMENTS: artifact name for a Mastering project
        RECORD_PAIR_VERIFIED_LABELS: artifact name for a Mastering project
        RECORD_PAIR_UNVERIFIED_LABELS: artifact name for a Mastering project
        RECORD_PAIR_ASSIGNMENTS: artifact name for a Mastering project
        CLUSTERING_MODEL: artifact name for a Mastering project
        PUBLISHED_CLUSTERS: artifact name for a Mastering project
        CLUSTER_RECORD_VERIFICATIONS: artifact name for a Mastering project
        CLUSTER_ASSIGNMENTS: artifact name for a Mastering project
    """

    # Schema Mapping artifacts
    UNIFIED_ATTRIBUTES: str = "UNIFIED_ATTRIBUTES"
    TRANSFORMATIONS: str = "TRANSFORMATIONS"
    SMR_MODEL: str = "SMR_MODEL"
    RECORD_COMMENTS: str = "RECORD_COMMENTS"
    # Mastering artifacts
    MASTERING_CONFIGURATION: str = "MASTERING_CONFIGURATION"
    USER_DEFINED_SIGNALS: str = "USER_DEFINED_SIGNALS"
    MASTERING_FUNCTIONS: str = "MASTERING_FUNCTIONS"
    RECORD_PAIR_COMMENTS: str = "RECORD_PAIR_COMMENTS"
    RECORD_PAIR_VERIFIED_LABELS: str = "RECORD_PAIR_VERIFIED_LABELS"
    RECORD_PAIR_UNVERIFIED_LABELS: str = "RECORD_PAIR_UNVERIFIED_LABELS"
    RECORD_PAIR_ASSIGNMENTS: str = "RECORD_PAIR_ASSIGNMENTS"
    CLUSTERING_MODEL: str = "CLUSTERING_MODEL"
    PUBLISHED_CLUSTERS: str = "PUBLISHED_CLUSTERS"
    CLUSTER_RECORD_VERIFICATIONS: str = "CLUSTER_RECORD_VERIFICATIONS"
    CLUSTER_ASSIGNMENTS: str = "CLUSTER_ASSIGNMENTS"


@dataclass()
class CategorizationArtifacts:
    """A dataclass representing artifact codes for Categorization projects in Tamr

    Args:
        UNIFIED_ATTRIBUTES: artifact name for a Categorization project
        TRANSFORMATIONS: artifact name for a Categorization project
        SMR_MODEL: artifact name for a Categorization project
        RECORD_COMMENTS: artifact name for a Categorization project

        CATEGORIZATION_CONFIGURATION: artifact name for a Categorization project
        CATEGORIZATION_FUNCTIONS: artifact name for a Categorization project
        CATEGORIZATION_VERIFIED_LABELS: artifact name for a Categorization project
        CATEGORIZATION_TAXONOMIES: artifact name for a Categorization project
        CATEGORIZATION_MODEL: artifact name for a Categorization project
        CATEGORIZATION_FEEDBACK: artifact name for a Categorization project
    """

    # Schema Mapping artifacts
    UNIFIED_ATTRIBUTES: str = "UNIFIED_ATTRIBUTES"
    TRANSFORMATIONS: str = "TRANSFORMATIONS"
    SMR_MODEL: str = "SMR_MODEL"
    RECORD_COMMENTS: str = "RECORD_COMMENTS"
    # Categorization artifacts
    CATEGORIZATION_CONFIGURATION: str = "CATEGORIZATION_CONFIGURATION"
    CATEGORIZATION_FUNCTIONS: str = "CATEGORIZATION_FUNCTIONS"
    CATEGORIZATION_VERIFIED_LABELS: str = "CATEGORIZATION_VERIFIED_LABELS"
    CATEGORIZATION_TAXONOMIES: str = "CATEGORIZATION_TAXONOMIES"
    CATEGORIZATION_MODEL: str = "CATEGORIZATION_MODEL"
    CATEGORIZATION_FEEDBACK: str = "CATEGORIZATION_FEEDBACK"


@dataclass()
class GoldenRecordsArtifacts:
    """A dataclass representing artifact codes for Golden Records projects in Tamr

    GR_CONFIGURATION: artifact name for a Golden Records project
    GR_RULES: artifact name for a Golden Records project
    GR_OVERRIDES: artifact name for a Golden Records project
    """

    # Golden Records artifacts
    GR_CONFIGURATION: str = "GR_CONFIGURATION"
    GR_RULES: str = "GR_RULES"
    GR_OVERRIDES: str = "GR_OVERRIDES"


@dataclass()
class ProjectArtifacts:
    """A dataclass representing the project artifact codes in Tamr

     Args:
        SCHEMA_MAPPING: SchemaMappingArtifacts dataclass instance
        MASTERING: MasteringArtifacts dataclass instance
        CATEGORIZATION: MasteringArtifacts dataclass instance
        GOLDEN_RECORDS: MasteringArtifacts dataclass instance
    """

    SCHEMA_MAPPING = SchemaMappingArtifacts()
    MASTERING = MasteringArtifacts()
    CATEGORIZATION = CategorizationArtifacts()
    GOLDEN_RECORDS = GoldenRecordsArtifacts()
