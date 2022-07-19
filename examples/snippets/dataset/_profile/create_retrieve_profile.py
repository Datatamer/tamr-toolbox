"""
An example script to create and/or retrieve a dataset profile
"""

import tamr_toolbox as tbox
from requests import HTTPError

# load example config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])

# Get dataset from Tamr instance
dataset_name = "<your dataset name>"
dataset = client.datasets.by_name(dataset_name)

try:
    profile = tbox.dataset.get_profile(dataset)
except HTTPError:
    # If the above command raises an error:
    # Can also rerun if the above command gives a warning that profile is out-of-date
    profile = tbox.dataset.get_profile(dataset, allow_create_or_refresh=True)
