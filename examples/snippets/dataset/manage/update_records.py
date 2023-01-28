"""
An example script to add and delete a dataset's records
"""
import tamr_toolbox as tbox

# load example multi config
my_config = tbox.utils.config.from_yaml("examples/resources/conf/migrate_dataset.config.yaml")

# Create the source tamr client
client = tbox.utils.client.create(**my_config["source_migration_instance"])

# Get dataset from Tamr instance
dataset_name = "<your dataset name>"
dataset = client.datasets.by_name(dataset_name)

"""
The following example upserts a sample record with a primary key of "27" and deletes an existing
record with a primary key of "14", assuming that the name of the primary key attribute is
"primaryKey".
"""
updates = [{"First_Name": ["John"], "Last_Name": ["Smith"]}, "delete"]
primary_keys = ["27", "14"]
dataset = tbox.dataset.manage.update_records(
    dataset, updates=updates, primary_keys=primary_keys, primary_key_name="primaryKey"
)

"""
The following example deletes the records having primary keys "7" and "13" in the same dataset as
above.
"""
primary_keys = ["7", "13"]
dataset = tbox.dataset.manage.update_records(
    dataset, delete_all=True, primary_keys=primary_keys, primary_key_name="primaryKey"
)
