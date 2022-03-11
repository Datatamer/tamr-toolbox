""" Example script for creating a copy of an existing project with a new name """

import os
from pathlib import Path
from typing import Dict, Any
import argparse
from argparse import Namespace
import tamr_toolbox as tbox
from tamr_toolbox import utils as tbu
from tamr_toolbox.utils.client import Client
from tamr_toolbox.utils.operation import Operation


def export_from_tamr(client: Client, *, project_name: str, export_path: str,) -> Operation:
    project = client.projects.by_name(project_name)
    if not export_path:
        export_path = os.path.join(
            Path(__file__).resolve().parent, f"project-movement/{project_name}"
        )
    return tbox.project.export_artifacts(project=project, artifact_directory_path=export_path)


def import_to_tamr(
    client: Client,
    *,
    zipfile_path: str,
    new_project_name: str,
    new_ud_name: str,
    overwrite_existing: bool = False,
) -> Operation:
    return tbox.project.import_artifacts(
        tamr_client=client,
        project_artifact_path=zipfile_path,
        new_project_name=new_project_name,
        new_unified_dataset_name=new_ud_name,
        overwrite_existing=overwrite_existing,
    )


def main(*, opts: Namespace, instance_connection_info: Dict[str, Any]):

    ## This block was added on 2022-02-25 to process encrypted pwd
    tamr_client = tbu.client.create(**instance_connection_info)

    # calling the action functions:
    # exporting the target project from tamr
    LOGGER.info(f"Project {opts.name} export from Tamr initializing...")
    op = export_from_tamr(tamr_client, project_name=opts.name, export_path=opts.exportPath)
    tbu.operation.enforce_success(op)
    LOGGER.info(op)

    ## preparing for the import
    # finding the path to export file
    zipfile_name = [
        f
        for f in os.listdir(opts.exportPath)
        if (os.path.isfile(os.path.join(opts.exportPath, f)) and f.endswith(".zip"))
    ][0]
    zipfile_path = f"{opts.exportPath}/{zipfile_name}"

    # constructing the new project name and respective unified dataset name to be imported to tamr
    new_project_name = opts.newName if opts.newName else f"{opts.name}{opts.postfix}"
    new_ud_name = opts.newUDName if opts.newUDName else f"{new_project_name}_unified_dataset"
    # importing a copy of target project to tamr
    LOGGER.info(f"Project {new_project_name} import to Tamr initializing...")
    op = import_to_tamr(
        tamr_client,
        zipfile_path=zipfile_path,
        new_project_name=new_project_name,
        new_ud_name=new_ud_name,
        overwrite_existing=opts.overwrite,
    )
    tbu.operation.enforce_success(op)
    LOGGER.info(op)
    LOGGER.info(
        f"Project {new_project_name} was successfully forked from Tamr project {opts.name}!"
    )


if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--name", default=None, required=True, help="raise to specify the name of target project",
    )
    parser.add_argument(
        "--exportPath",
        default=None,
        required=True,
        help="raise to specify the path to export directory",
    )
    parser.add_argument(
        "--newUDName",
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
        "--newName", default=None, help="raise to explicitly specify the name of the new project",
    )
    group.add_argument(
        "--postfix",
        default=None,
        help="raise to imply the name of the new project by postfixing the original project name",
    )
    opts = parser.parse_args()

    # creating the logger object:
    logging_dir = "."
    LOGGER = tbu.logger.create(__file__, log_directory=logging_dir)
    # Let Tamr Toolbox itself also contribute to the log
    tbu.logger.enable_toolbox_logging(log_directory=logging_dir, log_to_terminal=False)
    # Configure the logs from imported packages
    tbu.logger.enable_package_logging(
        "tamr_unify_client", log_directory=logging_dir, log_to_terminal=False
    )

    # load config file and create tamr client
    conf_dir = "."
    config = tbu.config.from_yaml(conf_dir)

    main(opts=opts, instance_connection_info=config["tamr"])
