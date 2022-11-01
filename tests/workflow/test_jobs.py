"""Tests for tasks related to running jobs for groups of Tamr projects"""
import pytest

from tamr_unify_client.project.resource import Project

from tamr_toolbox import workflow
from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_run_one():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    all_ops = workflow.jobs.run([project])

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 2

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert "Predict record categorizations" == all_ops[1].description


@mock_api()
def test_run_one_training():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    all_ops = workflow.jobs.run([project], run_apply_feedback=True)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 3

    assert (
        f"Materialize views [{project.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )
    assert (
        f"materialize {project.unified_dataset().name}_classification_model"
        == all_ops[1].description
    )
    assert "Predict record categorizations" == all_ops[2].description


@mock_api()
def test_run_many():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project1 = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    project2 = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    project3 = client.projects.by_resource_id(CONFIG["projects"]["minimal_golden_records"])
    all_ops = workflow.jobs.run([project1, project2, project3])

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 10

    # Schema mapping
    assert (
        f"Materialize views [{project1.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )

    # Mastering
    assert (
        f"Materialize views [{project2.unified_dataset().name}] to Elastic"
        == all_ops[1].description
    )
    assert "Update Pairs" == all_ops[2].description
    assert "Predict Pairs" == all_ops[3].description
    assert "Generate High-impact Pairs" == all_ops[4].description
    assert "Clustering" == all_ops[5].description
    assert "Publish clusters" == all_ops[6].description

    # Golden records
    assert "Updating all profiling information for Golden Records" == all_ops[7].description
    assert "Updating Golden Records" == all_ops[8].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[9].description


@mock_api()
def test_run_many_training():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project1 = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    project2 = client.projects.by_resource_id(CONFIG["projects"]["minimal_mastering"])
    project3 = client.projects.by_resource_id(CONFIG["projects"]["minimal_golden_records"])
    all_ops = workflow.jobs.run(
        [project1, project2, project3], run_apply_feedback=True, run_estimate_pair_counts=True
    )

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 12

    # Schema mapping
    assert (
        f"Materialize views [{project1.unified_dataset().name}] to Elastic"
        == all_ops[0].description
    )

    # Mastering
    assert (
        f"Materialize views [{project2.unified_dataset().name}] to Elastic"
        == all_ops[1].description
    )
    assert (
        all_ops[2].description == "Generate Pair Estimates"
        or len(all_ops[2].status["message"]) == 0  # Indicating a 204, already up to date
    )
    assert "Update Pairs" == all_ops[3].description
    assert "Train Mastering Model" == all_ops[4].description
    assert "Predict Pairs" == all_ops[5].description
    assert "Generate High-impact Pairs" == all_ops[6].description
    assert "Clustering" == all_ops[7].description
    assert "Publish clusters" == all_ops[8].description

    # Golden records
    assert "Updating all profiling information for Golden Records" == all_ops[9].description
    assert "Updating Golden Records" == all_ops[10].description
    assert "Updating published datasets for GoldenRecords module" == all_ops[11].description


@mock_api()
def test_run_error():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = Project.from_json(client, resource_json={"name": "fake", "type": "NOT_REAL"})

    with pytest.raises(KeyError):
        workflow.jobs.run([project])


@mock_api()
def test_run_with_profile():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
    all_ops = workflow.jobs.run([project], run_profile_unified_datasets=True)

    for op in all_ops:
        assert op.succeeded()

    assert len(all_ops) == 2

    if all_ops[1].relative_id.split("/")[-1] == "-1":
        assert (
            all_ops[1].description == "Tamr returned HTTP 204 for this operation, indicating "
            "that all\n                results that would be "
            "produced by the operation are already up-to-date."
        )
    else:
        assert (
            f"Profiling [{project.unified_dataset().name}] attributes." == all_ops[1].description
        )


@mock_api()
@pytest.mark.parametrize(
    "project_name,expected_number_of_output_datasets",
    [
        ("minimal_schema_mapping", 1),
        ("minimal_mastering", 36),
        ("minimal_golden_records", 7),
        ("minimal_categorization", 8),
        ("minimal_phone_enrichment", 1),
    ],
)
def test_get_project_output_datasets(project_name, expected_number_of_output_datasets):
    reference_postfix_dict = {
        "SCHEMA_MAPPING_RECOMMENDATIONS": [""],
        "DEDUP": [
            "",
            "_dedup_clusters",
            "_dedup_clusters_with_data",
            "_dedup_published_clusters_with_data",
        ],
        "GOLDEN_RECORDS": [
            "_golden_records",
            "_golden_records_draft",
            "_golden_records_rule_output",
        ],
        "CATEGORIZATION": ["", "_all_classifications", "_classifications_average_confidences"],
    }
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project = client.projects.by_name(project_name)
    output_dataset_names = [
        ds.name for ds in workflow.jobs.get_project_output_datasets(test_project)
    ]
    # expected number of output datasets for some project types, e.g., DEDUP
    # is too large to check all dataset names individually
    # alternatively, the number of expected output datasets is used for testing
    # at the time this test was developed TBOX TEST instance ran on v2022.010.2
    if expected_number_of_output_datasets:
        assert len(output_dataset_names) == expected_number_of_output_datasets
    # spot-checking to assert expected datasets are within the returned list
    if test_project.type != "ENRICHMENT":
        reference_dataset_name = (
            test_project.unified_dataset().name
            if not test_project.type == "GOLDEN_RECORDS"
            else test_project.name
        )
        list_of_expected_output_dataset_names = [
            f"{reference_dataset_name}{postfix}"
            for postfix in reference_postfix_dict.get(test_project.type, [])
        ]
    else:
        list_of_expected_output_dataset_names = ["validated_phone_numbers_enriched_dataset"]

    for expected_dataset_name in list_of_expected_output_dataset_names:
        assert expected_dataset_name in output_dataset_names
