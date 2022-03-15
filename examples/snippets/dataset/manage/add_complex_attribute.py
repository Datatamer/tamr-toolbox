"""
An example script to change dataset attributes for a Tamr dataset
"""
import tamr_toolbox as tbox
from tamr_unify_client.attribute.resource import AttributeSpec
from tamr_unify_client.attribute.type import AttributeType


# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Use the configuration to create a global logger
LOGGER = tbox.utils.logger.create(__name__, log_directory=my_config["logging_dir"])

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])


dataset_name = "<your dataset name>"
# Get dataset from Tamr instance
dataset = client.datasets.by_name(dataset_name)

# Note you can optionally just past in a list of attribute names
# The attribute types will default to ARRAY STRING
attribute_names = ["unique_id", "name", "address", "user_id", "sales"]
attribute_types = [
    {"baseType": "STRING"},
    {"baseType": "ARRAY", "innerType": {"baseType": "STRING"}},
    {"baseType": "ARRAY", "innerType": {"baseType": "STRING"}},
    {"baseType": "ARRAY", "innerType": {"baseType": "INT"}},
    {"baseType": "DOUBLE"},
]

attribute_specs = []
for idx in range(len(attribute_names)):
    name = attribute_names[idx]
    attribute_specs.append(
        AttributeSpec.new().with_name(name).with_type(AttributeType(attribute_types[idx]).spec())
    )

updated_dataset = tbox.data_io.manage_dataset.modify(dataset=dataset, attributes=attribute_specs,)
