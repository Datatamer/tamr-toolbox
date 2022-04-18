"""
An example script to change dataset attributes for a Tamr dataset
"""
import tamr_toolbox as tbox
from tamr_toolbox.models import attribute_type


# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])


dataset_name = "<your dataset name>"
# Get dataset from Tamr instance
dataset = client.datasets.by_name(dataset_name)

# Note you can optionally just past in a list of attribute names
# The attribute types will default to ARRAY STRING
attribute_names = ["client_id", "name", "address", "user_id", "sales", "location"]
attribute_types = [
    attribute_type.STRING,
    attribute_type.DEFAULT,
    attribute_type.DEFAULT,
    attribute_type.Array(attribute_type.INT),
    attribute_type.DOUBLE,
    attribute_type.GEOSPATIAL,
]

attr_type_dict = {}
for i in range(len(attribute_names)):
    attr_type_dict[attribute_names[i]] = attribute_types[i]

updated_dataset = tbox.dataset.manage.create_attributes(
    dataset=dataset, attributes=attribute_names, attribute_types=attr_type_dict
)
