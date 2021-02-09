"""
Simple script to wholly unmap a dataset and remove it from a project
Can be used for any project type that has a schema mapping element
(e.g. all of 'from tamr_toolbox.project.<mastering,categorization,schema_mapping> import schema'
will work)
"""
import tamr_toolbox as tbox
import click


@click.command()
@click.option("--config_file", help="the yaml config file used to set up tamr client")
@click.option("--project_name", help="the name of the project from which to remove the dataset")
@click.option("--source_dataset_name", help="the name of the dataset to unmap/remove")
def main(config_file: str, project_name: str, source_dataset_name: str) -> None:
    """
    Unmap a dataset and remove it from a project
    Args:
        config_file: path to the config file containing server information
        project_name: name of the project to renove a dataset from
        source_dataset_name: name of the dataset to remove

    Returns:

    """
    # setup logger
    logger = tbox.utils.logger.create("my_logger")

    # get config and setup client
    config = tbox.utils.config.from_yaml(config_file)
    client = tbox.utils.client.create(**config["my_tamr_instance"])

    # get dataset and project
    source_dataset = client.datasets.by_name(source_dataset_name)
    project = client.projects.by_name(project_name)

    logger.info(
        f"unmapping and removing dataset {source_dataset_name} from project {project_name}"
    )
    tbox.project.mastering.schema.unmap_dataset(
        project, source_dataset=source_dataset, remove_dataset_from_project=True
    )


if __name__ == "__main__":
    main()
