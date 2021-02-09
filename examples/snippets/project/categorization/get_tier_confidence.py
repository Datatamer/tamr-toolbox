"""Snippet for retrieving confidence metrics from a Tamr Categorization project"""
import tamr_toolbox as tbox
from tamr_toolbox.project.categorization.metrics import get_tier_confidence

# Read config, make Tamr Client, make logger
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Get a Tamr categorization project by ID
my_project = tamr.projects.by_resource_id("my_project_id")

# By default gets the average confidence at leaf nodes without allowing dataset to refresh
leaf_node_confidence_dict = get_tier_confidence(my_project)

# Can allow the dataset to refresh if it is not streamable
# NOTE THIS WILL KICK OFF A <MATERIALIZE VIEWS> JOB
leaf_node_confidence_dict2 = get_tier_confidence(my_project, allow_dataset_refresh=True)

# Can also set the specific tier, which starts at 1
tier1_confidence_dict = get_tier_confidence(my_project, tier=1)
