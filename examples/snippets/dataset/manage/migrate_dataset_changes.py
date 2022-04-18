"""
An example script to migrate changes to a dataset attributes from one instance of Tamr to another
"""
import tamr_toolbox as tbox

# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

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
    attr_type_dict = {
        attr.name: tbox.models.attribute_type.from_json(attr.spec().to_dict()["type"])
        for attr in source_dataset.attributes
    }
    attribute_names = attr_type_dict.keys()

    description = source_dataset.description
    tags = source_dataset.tags

    # Migrate dataset updates from source to target instance
    if tbox.dataset.manage.exists(client=target_client, dataset_name=dataset_name):
        target_dataset = target_client.datasets.by_name(dataset_name)
        tbox.dataset.manage.update(
            dataset=target_dataset,
            attributes=attribute_names,
            attribute_types=attr_type_dict,
            description=description,
            tags=tags,
            override_existing_types=True,
        )
    else:
        primary_keys = source_dataset.spec().to_dict()["keyAttributeNames"]
        tbox.dataset.manage.create(
            client=target_client, primary_keys=primary_keys, dataset=source_dataset,
        )
