"""Tests for tasks related to movement with Tamr projects"""
import pytest
import os
from tamr_toolbox import utils, workflow
from tests._common import get_toolbox_root_dir
from tamr_toolbox.project import categorization
from tamr_toolbox.models.project_artifacts import (
    SchemaMappingArtifacts,
    CategorizationArtifacts,
    MasteringArtifacts,
    GoldenRecordsArtifacts,
)
from tamr_unify_client import Client
from typing import List, Optional
from tamr_toolbox.utils.testing import mock_api
from datetime import datetime

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


# now = datetime.now() #.strftime("%d-%m-%Y_%H:%M:%S")


def _project_clean_up(client: Client, project_name: str, unified_dataset_name: str) -> List:
    # delete project and associated datasets (including not captured by cascade)
    responses = []
    try:
        # delete project
        project = client.projects.by_name(project_name)
        r = client.delete(f"/api/recipe/projects/{project.resource_id}")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")
    try:
        # delete unified dataset
        ud_id = client.datasets.by_name(unified_dataset_name).resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{ud_id}?cascade=true")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_1 = client.datasets.by_name(unified_dataset_name + "_categories").resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_1}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_2 = client.datasets.by_name(
            unified_dataset_name + "_manual_categorizations"
        ).resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_2}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_3 = client.datasets.by_name(
            unified_dataset_name + "_function_categorizations"
        ).resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_3}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_4 = client.datasets.by_name(unified_dataset_name + "_record_comments").resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_4}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_5 = client.datasets.by_name(
            unified_dataset_name + "_classification_histogram_boundaries"
        ).resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_5}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    try:
        id_6 = client.datasets.by_name(unified_dataset_name + "_classification_model").resource_id
        r = client.delete(f"/api/versioned/v1/datasets/{id_6}?cascade=false")
        responses.append(r)
    except Exception as e:
        print(f"Error: {str(e)}")

    return responses


@mock_api()
def test_export_errors():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])

    # test incorrect artifact name
    with pytest.raises(ValueError):
        categorization.export_artifacts(
            project=project,
            artifact_directory_path="/home/ubuntu/tamr/projectExports",
            exclude_artifacts=[
                CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS,
                CategorizationArtifacts.CATEGORIZATION_TAXONOMIES,
                "INCORRECT_ARTIFACT_NAME",
            ],
            asynchronous=False,
        )

    # test incorrect artifact directory path
    with pytest.raises(ValueError):
        categorization.export_artifacts(
            project=project,
            artifact_directory_path="/an/incorrect/path",
            exclude_artifacts=[
                CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS,
                CategorizationArtifacts.CATEGORIZATION_TAXONOMIES,
            ],
            asynchronous=False,
        )


@mock_api()
def test_import_new_errors():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])

    # export a project
    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path="/home/ubuntu/tamr/projectExports",
        exclude_artifacts=None,
        asynchronous=False,
    )

    assert op.succeeded()

    # get artifact path
    description = op.description
    artifact_path = str(description).split(" ")[-1]

    assert artifact_path[0] == "/"
    assert os.path.splitext(artifact_path)[1] == ".zip"

    # import new project if it doesn't exist already
    new_project_name = "new_categorization"
    if new_project_name not in [p.name for p in client.projects]:
        # test incorrect path
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path="incorrect/artifact/path",
                tamr_client=client,
                new_project_name=new_project_name,
            )
        # fail if not present
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                include_destructive_artifacts=[CategorizationArtifacts.UNIFIED_ATTRIBUTES],
                fail_if_not_present=True,
            )
        # testing incorrect artifact names
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                exclude_artifacts=["incorrect_artifact_name"],
            )
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                include_additive_artifacts=["incorrect_artifact_name"],
            )
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                include_destructive_artifacts=["incorrect_artifact_name"],
            )
        # test trying to write an existing project name
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name="minimal_incomplete_categorization",
            )
        # test trying to write an existing unified dataset name
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                new_unified_dataset_name="minimal_categorization_unified_dataset",
            )
        if new_project_name in [p.name for p in client.projects]:
            raise RuntimeError(f"{new_project_name} is being unintentionally created during test.")
    else:
        raise AssertionError(f"{new_project_name} already exists in test instance.")


@mock_api(enforce_online_test=True)
def test_import_existing_errors():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])

    # export a project
    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path="/home/ubuntu/tamr/projectExports",
        exclude_artifacts=None,
        asynchronous=False,
    )

    assert op.succeeded()

    # get artifact path
    description = op.description
    artifact_path = str(description).split(" ")[-1]
    print(artifact_path)

    assert artifact_path[0] == "/"
    assert os.path.splitext(artifact_path)[1] == ".zip"

    # get existing project
    existing_project = client.projects.by_name("minimal_incomplete_categorization")

    # test trying to set new_project name on existing project
    with pytest.raises(KeyError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            new_project_name="new_project_name",
            overwrite_existing=True,
        )

    # test trying to set new_unified_dataset_name on existing project
    with pytest.raises(KeyError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            new_unified_dataset_name="new_ud_name",
            overwrite_existing=True,
        )

    # test overwrite existing
    with pytest.raises(KeyError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path="incorrect/artifact/path",
            target_project=existing_project,
            overwrite_existing=False,
        )

    # test incorrect artifact path
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path="incorrect/artifact/path",
            target_project=existing_project,
            overwrite_existing=True,
        )

    # fail if not present
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            project_artifact_path=artifact_path,
            tamr_client=client,
            target_project=existing_project,
            include_destructive_artifacts=[CategorizationArtifacts.CATEGORIZATION_TAXONOMIES],
            fail_if_not_present=True,
            overwrite_existing=True,
        )

    # test incorrect artifact name
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            include_additive_artifacts=["incorrect_artifact_name"],
            overwrite_existing=True,
        )
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            include_destructive_artifacts=["incorrect_artifact_name"],
            overwrite_existing=True,
        )
    # Expected ValueError but got RuntimeError instead
    with pytest.raises(RuntimeError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            exclude_artifacts=["incorrect_artifact_name"],
            overwrite_existing=True,
        )
    # trying to include an additive artifact that is not supported
    # This is RuntimeError, not ValueError
    with pytest.raises(RuntimeError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            include_additive_artifacts=[CategorizationArtifacts.CATEGORIZATION_MODEL],
            overwrite_existing=True,
        )


@mock_api()
def test_export():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    path_export_dir = "/home/ubuntu/tamr/projectExports"
    exclude_list = [
        CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS,
        CategorizationArtifacts.CATEGORIZATION_FEEDBACK,
        CategorizationArtifacts.CATEGORIZATION_TAXONOMIES,
    ]

    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path=path_export_dir,
        exclude_artifacts=exclude_list,
        asynchronous=False,
    )

    assert op.succeeded()


@pytest.mark.parametrize(
    "project_to_export, new_project_name, "
    "include_additive_artifacts, include_destructive_artifacts",
    [
        (
            CONFIG["projects"]["minimal_categorization"],
            f"new_categorization_{datetime.now().strftime('%s')}",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_mastering"],
            f"new_mastering_{datetime.now().strftime('%s')}",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_golden_records"],
            f"new_golden_records_{datetime.now().strftime('%s')}",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_schema_mapping"],
            f"new_schema_mapping_{datetime.now().strftime('%s')}",
            None,
            None,
        ),
        (
            CONFIG["projects"]["schema_mapping_with_spaces_in_name"],
            f"new_sm_with_spaces_{datetime.now().strftime('%s')}",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_categorization"],
            f"new_categorization_{datetime.now().strftime('%s')}",
            [CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS],
            [
                CategorizationArtifacts.CATEGORIZATION_MODEL,
                CategorizationArtifacts.CATEGORIZATION_TAXONOMIES,
            ],
        ),
        (
            CONFIG["projects"]["minimal_mastering"],
            f"new_mastering_{datetime.now().strftime('%s')}",
            [MasteringArtifacts.RECORD_PAIR_VERIFIED_LABELS],
            [MasteringArtifacts.CLUSTERING_MODEL, MasteringArtifacts.PUBLISHED_CLUSTERS],
        ),
        (
            CONFIG["projects"]["minimal_golden_records"],
            f"new_golden_records_{datetime.now().strftime('%s')}",
            [GoldenRecordsArtifacts.GR_OVERRIDES],
            [GoldenRecordsArtifacts.GR_RULES],
        ),
        (
            CONFIG["projects"]["minimal_schema_mapping"],
            f"new_schema_mapping_{datetime.now().strftime('%s')}",
            [SchemaMappingArtifacts.UNIFIED_ATTRIBUTES],
            [SchemaMappingArtifacts.TRANSFORMATIONS],
        ),
    ],
)
@mock_api(enforce_online_test=True)
def test_import_new(
    project_to_export: str,
    new_project_name: str,
    include_additive_artifacts: Optional[List[str]],
    include_destructive_artifacts: Optional[List[str]],
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(project_to_export)

    # export a project
    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path="/home/ubuntu/tamr/projectExports",
        exclude_artifacts=None,
        asynchronous=False,
    )

    assert op.succeeded()

    # get artifact path
    description = op.description
    artifact_path = str(description).split(" ")[-1]

    assert artifact_path[0] == "/"
    assert os.path.splitext(artifact_path)[1] == ".zip"

    # import new project if it doesn't exist already
    if project.type == "GOLDEN_RECORDS":
        new_unified_dataset_name = None
    else:
        new_unified_dataset_name = new_project_name + "_ud"
    if new_project_name not in [p.name for p in client.projects]:
        op = categorization.import_artifacts(
            project_artifact_path=artifact_path,
            tamr_client=client,
            new_project_name=new_project_name,
            new_unified_dataset_name=new_unified_dataset_name,
            include_additive_artifacts=include_additive_artifacts,
            include_destructive_artifacts=include_destructive_artifacts,
            asynchronous=False,
        )
        assert op.succeeded()
    else:
        raise AssertionError(f"{new_project_name} already exists in test instance.")

    # run new project
    project = client.projects.by_name(new_project_name)
    # run jobs
    ops = workflow.jobs.run([project], run_apply_feedback=False)
    for op in ops:
        assert op.succeeded()
    # clean up delete project and associated datasets
    responses = _project_clean_up(client, new_project_name, new_unified_dataset_name)
    print(responses)


@pytest.mark.parametrize(
    "project_to_export, existing_project_name, "
    "include_additive_artifacts, include_destructive_artifacts",
    [
        (
            CONFIG["projects"]["minimal_categorization"],
            "minimal_incomplete_categorization",
            None,
            None,
        ),
        (CONFIG["projects"]["minimal_mastering"], "minimal_incomplete_mastering", None, None),
        (
            CONFIG["projects"]["minimal_golden_records"],
            "minimal_incomplete_golden_records",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_schema_mapping"],
            "minimal_incomplete_schema_mapping",
            None,
            None,
        ),
        (
            CONFIG["projects"]["minimal_categorization"],
            "minimal_incomplete_categorization",
            [CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS],
            [
                CategorizationArtifacts.CATEGORIZATION_MODEL,
                CategorizationArtifacts.CATEGORIZATION_TAXONOMIES,
            ],
        ),
        (
            CONFIG["projects"]["minimal_mastering"],
            "minimal_incomplete_mastering",
            [MasteringArtifacts.RECORD_PAIR_VERIFIED_LABELS],
            [MasteringArtifacts.CLUSTERING_MODEL, MasteringArtifacts.PUBLISHED_CLUSTERS],
        ),
        (
            CONFIG["projects"]["minimal_golden_records"],
            "minimal_incomplete_golden_records",
            None,
            [GoldenRecordsArtifacts.GR_RULES],
        ),
        (
            CONFIG["projects"]["minimal_schema_mapping"],
            "minimal_incomplete_schema_mapping",
            [SchemaMappingArtifacts.UNIFIED_ATTRIBUTES],
            [SchemaMappingArtifacts.TRANSFORMATIONS],
        ),
    ],
)
@mock_api(enforce_online_test=True)
def test_import_existing(
    project_to_export: str,
    existing_project_name: str,
    include_additive_artifacts: Optional[List[str]],
    include_destructive_artifacts: Optional[List[str]],
):
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    # project to export
    project = client.projects.by_resource_id(project_to_export)
    # export a project
    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path="/home/ubuntu/tamr/projectExports",
        exclude_artifacts=None,
        asynchronous=False,
    )
    assert op.succeeded()

    # get artifact path
    description = op.description
    artifact_path = str(description).split(" ")[-1]

    assert artifact_path[0] == "/"
    assert os.path.splitext(artifact_path)[1] == ".zip"

    # get existing project
    existing_project = client.projects.by_name(existing_project_name)

    # export existing project to regenerate after test
    op_existing = categorization.export_artifacts(
        project=existing_project,
        artifact_directory_path="/home/ubuntu/tamr/projectExports",
        exclude_artifacts=None,
        asynchronous=False,
    )
    description = op_existing.description
    artifact_path_existing = str(description).split(" ")[-1]

    # test import into existing project
    op = categorization.import_artifacts(
        tamr_client=existing_project.client,
        project_artifact_path=artifact_path,
        target_project=existing_project,
        include_additive_artifacts=include_additive_artifacts,
        include_destructive_artifacts=include_destructive_artifacts,
        overwrite_existing=True,
        asynchronous=False,
    )
    assert op.succeeded()

    # run target project
    ops = workflow.jobs.run([existing_project], run_apply_feedback=False)
    for op in ops:
        assert op.succeeded()
    # clean revert project to it's original state
    project_name = existing_project.name
    if existing_project.type == "GOLDEN_RECORDS":
        unified_dataset_name = None
    else:
        unified_dataset_name = existing_project.unified_dataset().name
    responses = _project_clean_up(client, project_name, unified_dataset_name)
    print(responses)
    if existing_project.name not in [p.name for p in client.projects]:
        op = categorization.import_artifacts(
            project_artifact_path=artifact_path_existing,
            tamr_client=client,
            new_project_name=project_name,
            new_unified_dataset_name=unified_dataset_name,
            asynchronous=False,
        )
        assert op.succeeded()
    else:
        raise AssertionError(f"{project_name} already exists in test instance.")
