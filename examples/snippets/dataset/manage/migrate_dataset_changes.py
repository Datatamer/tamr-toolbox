"""
An example script to migrate changes to a dataset attributes from one instance of Tamr to another
"""
import tamr_toolbox as tbox

# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Use the configuration to create a global logger
LOGGER = tbox.utils.logger.create(__name__, log_directory=my_config["logging_dir"])

# Create the source tamr client
source_client = tbox.utils.client.create(**my_config["source_migration_instance"])

# Create the target tamr client
target_client = tbox.utils.client.create(**my_config["target_migration_instance"])

# Update each dataset
datasets = my_config["datasets"]
for ds in datasets:
    dataset_name = ds["dataset_name"]

    # Get dataset from source instance
    source_dataset = source_client.datasets.by_name(dataset_name)

    # Get updated dataset definition
    attributes = [attr for attr in source_dataset.attributes.stream()]
    attribute_names = [attr.name for attr in attributes]
    attribute_types = [attr.spec().to_dict()["type"] for attr in attributes]
    description = source_dataset.description
    tags = source_dataset.tags
    target_dataset = target_client.datasets.by_name(dataset_name)

    # Migrate dataset updates from source to target instance
    tbox.data_io.manage_dataset.modify(
        dataset=target_dataset,
        attributes=attributes,
        attribute_types=attribute_types,
        description=description,
        tags=tags,
    )
