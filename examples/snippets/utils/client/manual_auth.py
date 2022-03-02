"""
this snippet shows how a tamr client object can be created
with manual passing of credentials during runtime.
"""
from tamr_toolbox import utils as tbu
from tamr_toolbox.utils.client import Client
from getpass import getpass


def manual_auth(config: dict) -> Client:
    """
    This function allows for setting up credentials in terminal when creating a unify client object
    - using the getpass function will conceal password phrase and requires manual human user input
    - this approach cannot be coupled with fully automated workflows (e.g., crobtab jobs)
    """

    config["username"] = input("Enter username: ")
    config["password"] = getpass("Enter password: ")

    return tbu.client.create(**config)


# eample implementation
config_path = "."
my_config = tbu.config.from_yaml(config_path)
tamr_client = manual_auth(my_config)
