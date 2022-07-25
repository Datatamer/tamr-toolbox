"""
An example script to create and/or retrieve a dataset profile
"""

import tamr_toolbox as tbox

# load example config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])

# Get dataset from Tamr instance
dataset_name = "<your dataset name>"
dataset = client.datasets.by_name(dataset_name)

"""
Setting the optional second argument to True below ensures that
    - A new profile is created if it does not exist or is in an inconsistent state
    - The profile is refreshed if it is out-of-date
"""
profile = tbox.dataset.get_profile(dataset, allow_create_or_refresh=True)
