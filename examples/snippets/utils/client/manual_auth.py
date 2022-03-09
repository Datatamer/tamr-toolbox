"""
Snippet for creating a tamr client object with manual passing of credentials during runtime.

Note: This approach cannot be coupled with fully automated workflows (e.g., crontab jobs)
"""

from tamr_toolbox import utils as tbu
from getpass import getpass


# load configuration variables that do not include AUTH credentials
config_path = "."
my_config = tbu.config.from_yaml(config_path)

# prompting user for AUTH info from the terminal
username = input("Enter username: ")
password = getpass("Enter password: ")  # the password will not be visible while being typed in the terminal

# creating the unify client object
tamr_client = tbu.client.create(username=username, password=password, **my_config)
