"""Example script for creating a copy of an existing project with a new name

To copy a project as "_copy"-postfixed name of the target project:
    python fork_project.py --export_path /path/to/export-file-dir/
                           --project_name <target_project_name>
                           --postfix _copy

To specify new project name explicitly:
    python fork_project.py --export_path /path/to/export-file-dir/
                           --project_name <target_project_name>
                           --new_name <new_project_name>
"""

import os
from pathlib import Path
from typing import Dict, Any
import argparse
import tamr_toolbox as tbox
from tamr_toolbox import utils as tbu
from tamr_toolbox.utils.client import Client
from tamr_toolbox.utils.operation import Operation


def export_from_tamr(client: Client, *, project_name: str, export_path: str,) -> Operation:
    """
    This function sets path for project artifacts export from Tamr and makes the call to execute
    the export action.
    Export path defaults to "project-movement/<project_name>" if no user-defined value is passed.py

    Args:
        client: an instance of Tamr unify client object
        project_name: name of the project to be exported from Tamr
        export_path: export path - must be accessible to the VM hosting Tamr

    Returns:
        operation for project export api call
    """
    project = client.projects.by_name(project_name)
    if not export_path:
        export_path = os.path.join(
            Path(__file__).resolve().parent, f"project-movement/{project_name}"
        )
    return tbox.project.export_artifacts(project=project, artifact_directory_path=export_path)


def main(
    *,
    project_name: str,
    new_name: str,
    postfix: str,
    new_ud_name: str,
    export_path: Path,
    overwrite: bool,
    instance_connection_info: Dict[str, Any],
):
    """
    This function creates a fork copy of a Tamr project by exporting the target project
    and importing it back to Tamr under a new name

    Args:
        project_name: name of the existing target project
        new_name: name of the forked project (Optional)
        postfix: if specified, will use and modify the target project name (Optional)
        new_ud_name: explicitly specify the name of unified dataset of forked project (Optional)
        export_path: export path - must be accessible to the VM hosting Tamr
        overwrite: flag to overwrite existing project artifacts
        instance_connection_info: Tamr instance & AUTH configs
    """

    tamr_client = tbu.client.create(**instance_connection_info)

    # calling the action functions:
    # exporting the target project from tamr
    LOGGER.info(f"Project {project_name} export from Tamr initializing...")
    op = export_from_tamr(tamr_client, project_name=project_name, export_path=export_path)
    tbu.operation.enforce_success(op)
    LOGGER.info(op)

    ## preparing for the import
    # finding the path to export file
    zipfile_name = [
        f
        for f in os.listdir(export_path)
        if (os.path.isfile(os.path.join(export_path, f)) and f.endswith(".zip"))
    ][0]
    zipfile_path = f"{export_path}/{zipfile_name}"

    # constructing the new project name and respective unified dataset name to be imported to tamr
    new_project_name = new_name if new_name else f"{project_name}{postfix}"
    new_ud_name = new_ud_name if new_ud_name else f"{new_project_name}_unified_dataset"
    # importing a copy of target project to tamr
    LOGGER.info(f"Project {new_project_name} import to Tamr initializing...")
    op = tbox.project.import_artifacts(
        tamr_client=tamr_client,
        project_artifact_path=zipfile_path,
        new_project_name=new_project_name,
        new_unified_dataset_name=new_ud_name,
        overwrite_existing=overwrite,
    )
    tbu.operation.enforce_success(op)
    LOGGER.info(op)
    LOGGER.info(
        f"Project {new_project_name} was successfully forked from Tamr project {project_name}!"
    )


if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="path to a YAML configuration file", required=False)
    parser.add_argument(
        "--project_name",
        default=None,
        required=True,
        help="raise to specify the name of target project",
    )
    parser.add_argument(
        "--export_path",
        default=None,
        required=True,
        help="raise to specify the path to export directory",
    )
    parser.add_argument(
        "--new_ud_name",
        default=None,
        help="raise to explicitly specify the name of forked project unified dataset (optional)",
    )
    parser.add_argument(
        "--overwrite",
        default=False,
        action="store_true",
        help="if raised will replace a project with specified target name (if one already exists)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--new_name", default=None, help="raise to explicitly specify the name of the new project",
    )
    group.add_argument(
        "--postfix",
        default=None,
        help="raise to imply the name of the new project by postfixing the original project name",
    )
    opts = parser.parse_args()

    # load config file and create tamr client
    CONFIG = tbox.utils.config.from_yaml(
        path_to_file=opts.config, default_path_to_file="/path/to/my/conf/project.config.yaml"
    )
    # creating the logger object:
    LOGGER = tbox.utils.logger.create(__name__, log_directory=CONFIG["logging_dir"])
    # Let Tamr Toolbox itself also contribute to the log
    tbu.logger.enable_toolbox_logging(log_directory=CONFIG["logging_dir"], log_to_terminal=False)

    main(
        project_name=opts.project_name,
        new_name=opts.new_name,
        postfix=opts.postfix,
        new_ud_name=opts.new_ud_name,
        export_path=opts.export_path,
        overwrite=opts.overwrite,
        instance_connection_info=CONFIG["tamr"],
    )
