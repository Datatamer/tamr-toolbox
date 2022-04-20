"""
An example script to delete attributes from a Tamr dataset
"""
import tamr_toolbox as tbox

# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])

# Get dataset from Tamr instance
dataset_name = "<your dataset name>"
dataset = client.datasets.by_name(dataset_name)


attribute_name = ["location"]

updated_dataset = tbox.dataset.manage.delete_attributes(
    dataset=dataset, attributes=[attribute_name]
)
