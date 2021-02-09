"""
Simple script to add a dataset and perform mappings via list of tuples or optionally bootstrap the
entire dataset.
Can be used for any project type that has a schema mapping element
(e.g. all of 'from tamr_toolbox.project.<mastering,categorization,schema_mapping> import schema'
will work)
"""
import tamr_toolbox as tbox
import click


@click.command()
@click.option("--config_file", help="the yaml config file used to set up tamr client")
@click.option("--project_name", help="the name of the project to which to add the dataset")
@click.option("--source_dataset_name", help="the name of the dataset to map")
@click.option(
    "--bootstrap", help="flag for whether or not to bootstrap the entire dataset", is_flag=True
)
@click.option(
    "--mappings",
    help="list of mappings to apply in format "
    "source_attr1,unified_attr1;source_attr2,unified_attr2",
    default="",
)
def main(
    config_file: str, project_name: str, source_dataset_name: str, bootstrap: bool, mappings: str
) -> None:
    """
    Add a Tamr dataset to a Tamr project and optionally bootstrap it or map it to the unified
    dataset following given attributes mapping

    Args:
        config_file: path to the config file containing server information
        project_name: name of the project to add a dataset to
        source_dataset_name: name of the dataset to add
        bootstrap: flag to boostrap the entire dataset to the unified dataset of the project
        mappings: mappings to use to map the source dataset to the unified dataset, mappings
            should follow the format "source_attr1,unified_attr1;source_attr2,unified_attr2"

    Returns:

    """

    # setup logger
    logger = tbox.utils.logger.create("my_logger")

    # get config and setup client
    config = tbox.utils.config.from_yaml(config_file)
    client = tbox.utils.client.create(**config["my_tamr_instance"])

    # grab project and source dataset
    project = client.projects.by_name(project_name)
    source_dataset = client.datasets.by_name(source_dataset_name)

    # if bootstrap then call bootstrap function with flag to add dataset to project if it
    # isn't already in
    if bootstrap:
        logger.info(f"bootstrapping dataset {source_dataset_name} in project {project_name}")
        tbox.project.mastering.schema.bootstrap_dataset(
            project, source_dataset=source_dataset, force_add_dataset_to_project=True
        )
        # if mappings is empty string we are done
        if mappings == "":
            logger.info("bootstrapped and mappings are empty so finishing")
            return None
    else:
        if mappings == "":
            logger.warning(
                "bootstrap not chosen but no mappings specified so exiting without doing anything"
            )
            return None

    # not bootstrap, manually add, and do mappings
    logger.info(
        f"bootstrap not chosen so manually adding {source_dataset_name} to project {project_name}"
    )
    project.add_input_dataset(source_dataset)
    # parse mapping tuples
    try:
        mapping_tuples = [(x.split(",")[0], x.split(",")[1]) for x in mappings.split(";")]
    except Exception as e:
        error_message = (
            f"Provided mappings do not follow the format "
            f"'source_attr1,unified_attr1;source_attr2,unified_attr2', error: {e}"
        )
        logger.error(error_message)
        raise RuntimeError(error_message)

    for (source_attr, unified_attr) in mapping_tuples:
        logger.debug(f"applying the following mapping: {source_attr} --> {unified_attr}")
        tbox.project.mastering.schema.map_attribute(
            project,
            source_attribute_name=source_attr,
            source_dataset_name=source_dataset.name,
            unified_attribute_name=unified_attr,
        )


if __name__ == "__main__":
    main()
