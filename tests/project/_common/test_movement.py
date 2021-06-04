"""Tests for tasks related to movement with Tamr projects"""
import pytest
from tamr_toolbox import utils
from tests._common import get_toolbox_root_dir
from tamr_toolbox.project import categorization
from tamr_toolbox.models.project_artifacts import CategorizationArtifacts as catfacts
from tamr_unify_client.project.resource import Project

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test_temp.yaml"
)


def _delete_imported_project(project: Project, cascade: bool) -> None:
    try:
        project.unified_dataset().delete(cascade=cascade)
    except Exception as err:
        error_message = f"Error deleting unified dataset: {str(err)}"
        raise Exception(error_message)
    try:
        project.delete()
    except Exception as err:
        error_message = f"Error deleting project: {str(err)}"
        raise Exception(error_message)


def test_export_errors():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])
    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])

    # test incorrect artifact name
    with pytest.raises(ValueError):
        categorization.export_artifacts(
            project=project,
            artifact_directory_path="/home/ubuntu/tamr/projectExports",
            exclude_artifacts=[
                catfacts.CATEGORIZATION_VERIFIED_LABELS,
                catfacts.CATEGORIZATION_CONFIGURATION,
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
                catfacts.CATEGORIZATION_VERIFIED_LABELS,
                catfacts.CATEGORIZATION_CONFIGURATION,
            ],
            asynchronous=False,
        )

    # test incompatible project type
    with pytest.raises(ValueError):
        wrong_type_project = client.projects.by_resource_id(
            CONFIG["projects"]["minimal_mastering"]
        )
        categorization.export_artifacts(
            project=wrong_type_project,
            artifact_directory_path="/home/ubuntu/tamr/projectExports",
            exclude_artifacts=[
                catfacts.CATEGORIZATION_VERIFIED_LABELS,
                catfacts.CATEGORIZATION_CONFIGURATION,
            ],
            asynchronous=False,
        )


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
    print(artifact_path)

    assert artifact_path[0] == "/"
    assert artifact_path[-4:] == ".zip"

    # import new project if it doesn't exist already
    new_project_name = "new_categorization"
    if new_project_name not in [p.name for p in client.projects.stream()]:
        # test incorrect path
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path="incorrect/artifact/path",
                tamr_client=client,
                new_project_name=new_project_name,
            )

        # test trying to write an existing unified dataset name
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                new_unified_dataset_name="minimal_categorization_unified_dataset",
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

        # fail if not present
        with pytest.raises(ValueError):
            categorization.import_artifacts(
                project_artifact_path=artifact_path,
                tamr_client=client,
                new_project_name=new_project_name,
                include_destructive_artifacts=[catfacts.UNIFIED_ATTRIBUTES],
                fail_if_not_present=True,
            )
    else:
        raise AssertionError(f"{new_project_name} already exists in test instance.")


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
    assert artifact_path[-4:] == ".zip"

    # get existing project
    existing_project = client.projects.by_resource_id(
        CONFIG["projects"]["minimal_incomplete_categorization"]
    )

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

    # test incorrect artifact name
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            tamr_client=existing_project.client,
            project_artifact_path=artifact_path,
            target_project=existing_project,
            exclude_artifacts=["incorrect_artifact_name"],
            overwrite_existing=True,
        )
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

    # fail if not present
    with pytest.raises(ValueError):
        categorization.import_artifacts(
            project_artifact_path=artifact_path,
            tamr_client=client,
            target_project=existing_project,
            include_destructive_artifacts=[catfacts.CATEGORIZATION_TAXONOMIES],
            fail_if_not_present=True,
            overwrite_existing=True,
        )


def test_export():
    client = utils.client.create(**CONFIG["toolbox_test_instance"])

    project = client.projects.by_resource_id(CONFIG["projects"]["minimal_categorization"])
    path_export_dir = "/home/ubuntu/tamr/projectExports"
    exclude_list = [
        catfacts.CATEGORIZATION_VERIFIED_LABELS,
        catfacts.CATEGORIZATION_FEEDBACK,
        catfacts.CATEGORIZATION_TAXONOMIES,
    ]

    op = categorization.export_artifacts(
        project=project,
        artifact_directory_path=path_export_dir,
        exclude_artifacts=exclude_list,
        asynchronous=False,
    )

    assert op.succeeded()


def test_import_new():
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
    assert artifact_path[-4:] == ".zip"

    # import new project if it doesn't exist already
    new_project_name = "new_categorization"
    if new_project_name not in [p.name for p in client.projects.stream()]:
        op = categorization.import_artifacts(
            project_artifact_path=artifact_path,
            tamr_client=client,
            new_project_name=new_project_name,
            new_unified_dataset_name=new_project_name + "_ud",
            asynchronous=False,
        )
        assert op.succeeded()
    else:
        raise AssertionError(f"{new_project_name} already exists in test instance.")

    # clean up delete project


def test_import_existing():
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
    assert artifact_path[-4:] == ".zip"

    # get existing project
    existing_project = client.projects.by_resource_id(
        CONFIG["projects"]["minimal_incomplete_categorization"]
    )

    # test trying to set new_project name on existing project
    categorization.import_artifacts(
        tamr_client=existing_project.client,
        project_artifact_path=artifact_path,
        target_project=existing_project,
        overwrite_existing=True,
        asynchronous=False,
    )
    assert op.succeeded()

    # clean revert project to it's original state


test_export()
test_import_new()
test_import_existing()

# export exceptions [-/]
# import exceptions [ ]

# new_project_name [-/]

# check orphaned unified [-/]

# test that project runs after import

### GARBAGE ###
# tamr_unify_client.operation.Operation(
# relative_id='operations/projectExport-minimal_categorization-2021-05-26_22-12-59-479',
# description=
#   'projectExport with artifact: /home/ubuntu/tamr/
#       projectExports/minimal_categorization-1622067179477.zip',
# state='SUCCEEDED')
# tamr_unify_client.operation.Operation(
# relative_id='operations/projectExport-minimal_categorization-2021-05-26_22-20-52-354',
# description=
#   'projectExport with artifact: /home/ubuntu/tamr/projectExports/
#       minimal_categorization-1622067652351.zip',
# state='RUNNING')
