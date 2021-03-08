"""Tests for tasks related to getting downstream artifacts"""
import tamr_toolbox
from tests._common import get_toolbox_root_dir
from tamr_toolbox.utils.testing import mock_api

CONFIG = tamr_toolbox.utils.config.from_yaml(
    f"{get_toolbox_root_dir()}/tests/mocking/resources/toolbox_test.yaml"
)
MASTERING_UNIFIED_DATASET_ID = CONFIG["datasets"]["minimal_mastering_unified_dataset"]
SOURCE_DATASET_ID = CONFIG["datasets"]["people_tiny.csv"]


@mock_api()
def test_get_dataset_downstream_dependencies():
    client = tamr_toolbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(MASTERING_UNIFIED_DATASET_ID)
    downstream_datasets = tamr_toolbox.utils.downstream.datasets(dataset)
    # check that these datasets are included in the list of downstream datasets
    PIPELINE_DEPENDENT_DATASETS = {
        "minimal_mastering_unified_dataset_dedup_user_defined_signals",
        "minimal_golden_records_source_list_diff",
        "minimal_mastering_unified_dataset_dedup_clusters_union",
        "minimal_mastering_unified_dataset_dedup_dnf_binning",
        "minimal_mastering_unified_dataset_dedup_cluster_stats_union",
        "minimal_golden_records_golden_records_cluster_profile",
        "minimal_mastering_unified_dataset_dedup_published_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_pair_comments",
        "minimal_mastering_unified_dataset_dedup_function_predictions",
        "minimal_mastering_unified_dataset_important_pairs",
        "minimal_mastering_unified_dataset_dedup_features",
        "minimal_golden_records_golden_records",
        "minimal_mastering_unified_dataset_dedup_clusters_with_stats_union",
        "minimal_mastering_unified_dataset_dedup_cluster_accuracy_metrics",
        "minimal_mastering_unified_dataset_dedup_idf",
        "minimal_mastering_unified_dataset_dedup_labels",
        "minimal_mastering_unified_dataset_dedup_clusters",
        "minimal_mastering_unified_dataset_dedup_published_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_test_records",
        "minimal_mastering_unified_dataset_dedup_training_clusters",
        "minimal_mastering_unified_dataset_dedup_published_clusters",
        "minimal_mastering_unified_dataset_dedup_test_clusters",
        "minimal_mastering_unified_dataset_dedup_feedback",
        "minimal_golden_records_golden_records_draft",
        "minimal_mastering_unified_dataset_dedup_high_impact_questions",
        "minimal_mastering_unified_dataset_dedup_test_records_accuracy",
        "minimal_mastering_unified_dataset_dedup_model",
        "minimal_golden_records_source_list",
        "minimal_mastering_unified_dataset_dedup_all_persistent_ids",
        "minimal_mastering_unified_dataset_dedup_human_signals",
        "minimal_mastering_unified_dataset_dedup_cluster_similarities",
        "minimal_mastering_unified_dataset_dedup_non_null_count",
        "minimal_mastering_unified_dataset_dedup_dnf_signals",
        "minimal_mastering_unified_dataset_dedup_published_cluster_counts",
        "minimal_golden_records_golden_records_pinned_cluster_input",
        "minimal_mastering_unified_dataset_dedup_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_signals_predictions",
        "minimal_golden_records_golden_records_rule_output",
        "minimal_mastering_unified_dataset_dedup_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_signals",
    }
    assert PIPELINE_DEPENDENT_DATASETS == set([d.name for d in downstream_datasets])


@mock_api()
def test_get_dataset_downstream_dependencies_suggest_name():
    client = tamr_toolbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(MASTERING_UNIFIED_DATASET_ID)
    downstream_datasets = tamr_toolbox.utils.downstream.datasets(
        dataset, include_dependencies_by_name=True
    )
    # check returned results
    # in newer version of Tamr, this number might increase
    # because more persistence data is moved to HBase
    assert len(downstream_datasets) == 49
    PIPELINE_DEPENDENT_DATASETS = {
        "minimal_mastering_unified_dataset_dedup_user_defined_signals",
        "minimal_golden_records_source_list_diff",
        "minimal_mastering_unified_dataset_dedup_clusters_union",
        "minimal_mastering_unified_dataset_dedup_dnf_binning",
        "minimal_mastering_unified_dataset_dedup_cluster_stats_union",
        "minimal_golden_records_golden_records_cluster_profile",
        "minimal_mastering_unified_dataset_dedup_published_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_pair_comments",
        "minimal_mastering_unified_dataset_dedup_function_predictions",
        "minimal_mastering_unified_dataset_important_pairs",
        "minimal_mastering_unified_dataset_dedup_features",
        "minimal_golden_records_golden_records",
        "minimal_mastering_unified_dataset_dedup_clusters_with_stats_union",
        "minimal_mastering_unified_dataset_dedup_cluster_accuracy_metrics",
        "minimal_mastering_unified_dataset_dedup_idf",
        "minimal_mastering_unified_dataset_dedup_labels",
        "minimal_mastering_unified_dataset_dedup_clusters",
        "minimal_mastering_unified_dataset_dedup_published_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_test_records",
        "minimal_mastering_unified_dataset_dedup_training_clusters",
        "minimal_mastering_unified_dataset_dedup_published_clusters",
        "minimal_mastering_unified_dataset_dedup_test_clusters",
        "minimal_mastering_unified_dataset_dedup_feedback",
        "minimal_golden_records_golden_records_draft",
        "minimal_mastering_unified_dataset_dedup_high_impact_questions",
        "minimal_mastering_unified_dataset_dedup_test_records_accuracy",
        "minimal_mastering_unified_dataset_dedup_model",
        "minimal_golden_records_source_list",
        "minimal_mastering_unified_dataset_dedup_all_persistent_ids",
        "minimal_mastering_unified_dataset_dedup_human_signals",
        "minimal_mastering_unified_dataset_dedup_cluster_similarities",
        "minimal_mastering_unified_dataset_dedup_non_null_count",
        "minimal_mastering_unified_dataset_dedup_dnf_signals",
        "minimal_mastering_unified_dataset_dedup_published_cluster_counts",
        "minimal_golden_records_golden_records_pinned_cluster_input",
        "minimal_mastering_unified_dataset_dedup_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_signals_predictions",
        "minimal_golden_records_golden_records_rule_output",
        "minimal_mastering_unified_dataset_dedup_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_signals",
    }
    DEPENDENT_DATASETS_BY_NAME = {
        "minimal_mastering_unified_dataset_dedup_verified_cluster_members",
        "minimal_mastering_unified_dataset.recordPairLabel",
        "minimal_mastering_unified_dataset.record_pair_feedback_uid",
        "minimal_mastering_unified_dataset.record_pair_id_string",
        "minimal_mastering_unified_dataset.internal_links",
        "minimal_mastering_unified_dataset_dedup_suggested_clusters_log",
        "minimal_mastering_unified_dataset.cluster_feedback",
        "minimal_mastering_unified_dataset.internal_comments",
        "minimal_mastering_unified_dataset.global_record_id",
    }
    assert PIPELINE_DEPENDENT_DATASETS.union(DEPENDENT_DATASETS_BY_NAME) == set(
        [d.name for d in downstream_datasets]
    )


@mock_api()
def test_get_downstream_projects():
    client = tamr_toolbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    dataset = client.datasets.by_resource_id(MASTERING_UNIFIED_DATASET_ID)
    downstream_projects = tamr_toolbox.utils.downstream.projects(
        dataset, include_dependencies_by_name=True
    )
    assert len(downstream_projects) == 2
    assert downstream_projects[0].relative_id == "projects/1"
    assert downstream_projects[1].relative_id == "projects/2"


@mock_api()
def test_get_downstream_datasets_for_source_dataset():
    client = tamr_toolbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_resource_id(SOURCE_DATASET_ID)
    source_downstream_datasets = tamr_toolbox.utils.downstream.datasets(source_dataset)
    source_downstream_datasets_names = {d.name for d in source_downstream_datasets}
    # List of downstream datasets of a source dataset includes
    # all derived datasets from projects the source dataset is added as input
    SOURCE_DATASET_DEPENDENCIES = {
        "minimal_mastering_unified_dataset_dedup_cluster_similarities",
        "minimal_mastering_unified_dataset_important_pairs",
        "minimal_mastering_unified_dataset_dedup_high_impact_questions",
        "minimal_schema_mapping_unified_dataset",
        "minimal_mastering_unified_dataset_dedup_pair_comments",
        "minimal_mastering_unified_dataset_dedup_test_clusters",
        "minimal_mastering_unified_dataset_dedup_user_defined_signals",
        "minimal_mastering_unified_dataset_dedup_published_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_signals_predictions",
        "minimal_mastering_unified_dataset_dedup_human_signals",
        "minimal_mastering_unified_dataset",
        "chained_minimal_schema_mapping_unified_dataset",
        "minimal_mastering_unified_dataset_dedup_all_persistent_ids",
        "minimal_mastering_unified_dataset_dedup_cluster_accuracy_metrics",
        "minimal_golden_records_golden_records_cluster_profile",
        "minimal_mastering_unified_dataset_dedup_cluster_stats_union",
        "minimal_mastering_unified_dataset_dedup_clusters_union",
        "minimal_mastering_unified_dataset_dedup_training_clusters",
        "minimal_mastering_unified_dataset_dedup_non_null_count",
        "minimal_mastering_unified_dataset_dedup_test_records",
        "minimal_mastering_unified_dataset_dedup_labels",
        "minimal_mastering_unified_dataset_dedup_function_predictions",
        "minimal_mastering_unified_dataset_dedup_published_cluster_counts",
        "minimal_mastering_unified_dataset_dedup_clusters_with_stats_union",
        "people_tiny.csv_sample",
        "minimal_mastering_unified_dataset_dedup_idf",
        "minimal_mastering_unified_dataset_dedup_features",
        "minimal_golden_records_golden_records",
        "minimal_golden_records_golden_records_rule_output",
        "minimal_mastering_unified_dataset_dedup_signals",
        "minimal_mastering_unified_dataset_dedup_clusters",
        "minimal_mastering_unified_dataset_dedup_published_clusters",
        "minimal_mastering_unified_dataset_dedup_dnf_binning",
        "minimal_mastering_unified_dataset_dedup_test_records_accuracy",
        "minimal_mastering_unified_dataset_dedup_dnf_signals",
        "minimal_golden_records_golden_records_pinned_cluster_input",
        "minimal_mastering_unified_dataset_dedup_feedback",
        "minimal_mastering_unified_dataset_dedup_published_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_model",
        "minimal_golden_records_source_list",
        "minimal_golden_records_source_list_diff",
        "minimal_mastering_unified_dataset_dedup_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_clusters_with_data",
        "minimal_golden_records_golden_records_draft",
    }
    assert source_downstream_datasets_names == SOURCE_DATASET_DEPENDENCIES


@mock_api()
def test_get_downstream_datasets_for_source_dataset_suggest_name():
    client = tamr_toolbox.utils.client.create(**CONFIG["toolbox_test_instance"])
    source_dataset = client.datasets.by_resource_id(SOURCE_DATASET_ID)
    source_downstream_datasets = tamr_toolbox.utils.downstream.datasets(
        source_dataset, include_dependencies_by_name=True
    )
    source_downstream_datasets_names = {d.name for d in source_downstream_datasets}
    # When `include_dependencies_by_name=True`, need to return
    # dependencies suggested by name for unified datasets that are downstream
    # of a source dataset
    SOURCE_DATASET_DEPENDENCIES_SUGGEST_NAME = {
        "minimal_mastering_unified_dataset_dedup_cluster_similarities",
        "minimal_mastering_unified_dataset_important_pairs",
        "minimal_mastering_unified_dataset_dedup_verified_cluster_members",
        "minimal_mastering_unified_dataset_dedup_high_impact_questions",
        "minimal_schema_mapping_unified_dataset",
        "minimal_mastering_unified_dataset.recordPairLabel",
        "minimal_mastering_unified_dataset.internal_links",
        "minimal_mastering_unified_dataset_dedup_pair_comments",
        "minimal_mastering_unified_dataset_dedup_test_clusters",
        "minimal_mastering_unified_dataset_dedup_user_defined_signals",
        "minimal_mastering_unified_dataset_dedup_published_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_signals_predictions",
        "minimal_mastering_unified_dataset_dedup_human_signals",
        "minimal_mastering_unified_dataset",
        "chained_minimal_schema_mapping_unified_dataset",
        "minimal_mastering_unified_dataset_dedup_all_persistent_ids",
        "minimal_mastering_unified_dataset_dedup_cluster_accuracy_metrics",
        "minimal_golden_records_golden_records_cluster_profile",
        "minimal_mastering_unified_dataset.record_pair_id_string",
        "minimal_mastering_unified_dataset_dedup_cluster_stats_union",
        "minimal_mastering_unified_dataset_dedup_clusters_union",
        "minimal_mastering_unified_dataset_dedup_training_clusters",
        "minimal_mastering_unified_dataset_dedup_non_null_count",
        "minimal_mastering_unified_dataset_dedup_test_records",
        "minimal_mastering_unified_dataset_dedup_labels",
        "minimal_mastering_unified_dataset_dedup_function_predictions",
        "minimal_mastering_unified_dataset.cluster_feedback",
        "minimal_mastering_unified_dataset_dedup_published_cluster_counts",
        "minimal_mastering_unified_dataset_dedup_clusters_with_stats_union",
        "people_tiny.csv_sample",
        "minimal_mastering_unified_dataset.internal_comments",
        "minimal_mastering_unified_dataset_dedup_idf",
        "minimal_golden_records_golden_records",
        "minimal_mastering_unified_dataset_dedup_features",
        "minimal_mastering_unified_dataset.record_pair_feedback_uid",
        "minimal_golden_records_golden_records_rule_output",
        "minimal_mastering_unified_dataset_dedup_signals",
        "minimal_mastering_unified_dataset_dedup_clusters",
        "minimal_mastering_unified_dataset_dedup_suggested_clusters_log",
        "minimal_mastering_unified_dataset_dedup_published_clusters",
        "minimal_mastering_unified_dataset.global_record_id",
        "minimal_mastering_unified_dataset_dedup_dnf_binning",
        "minimal_mastering_unified_dataset_dedup_test_records_accuracy",
        "minimal_mastering_unified_dataset_dedup_dnf_signals",
        "minimal_golden_records_golden_records_pinned_cluster_input",
        "minimal_mastering_unified_dataset_dedup_feedback",
        "minimal_mastering_unified_dataset_dedup_published_clusters_with_data",
        "minimal_mastering_unified_dataset_dedup_model",
        "minimal_golden_records_source_list",
        "minimal_golden_records_source_list_diff",
        "minimal_mastering_unified_dataset_dedup_cluster_stats",
        "minimal_mastering_unified_dataset_dedup_clusters_with_data",
        "minimal_golden_records_golden_records_draft",
    }
    assert source_downstream_datasets_names == SOURCE_DATASET_DEPENDENCIES_SUGGEST_NAME
