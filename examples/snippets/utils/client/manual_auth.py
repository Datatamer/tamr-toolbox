"""
Snippet for creating a tamr client object with manual passing of credentials during runtime.

a. using the getpass function will conceal password phrase and requires manual human user input
b. this approach cannot be coupled with fully automated workflows (e.g., crontab jobs)
c. it is presumed that when setting AUTH manually the config.yaml file does not contain the
   username and password fields
"""

from tamr_toolbox import utils as tbu
from getpass import getpass


# load configuration variables that do not include AUTH credentials
config_path = "."
my_config = tbu.config.from_yaml(config_path)

# prompting user for AUTH info from the terminal
username = input("Enter username: ")
password = getpass("Enter password: ")

# creating the unify client object
tamr_client = tbu.client.create(username=username, password=password, **my_config)
