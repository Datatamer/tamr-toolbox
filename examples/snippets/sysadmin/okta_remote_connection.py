"""Connect to a remote Tamr when SSH is proxied via OKTA Advanced Server Access"""

import paramiko
import os
import subprocess
import tamr_toolbox as tbox

# Set your connection parameters
hostname = "10.99.9.999"
username = "my.username"

# Login to Okta ASA.
# If there is no current session a browser window will open to complete authentication
subprocess.run(["sft", "login"])

# Get ssh proxy information from ssh config file and use it to create a connection client
ssh_config = paramiko.SSHConfig()
with open(os.path.expanduser("~/.ssh/config")) as ssh_config_file:
    ssh_config.parse(ssh_config_file)
host_ssh_config = ssh_config.lookup(hostname)
remote_client = paramiko.SSHClient()
remote_client.load_host_keys(host_ssh_config["userknownhostsfile"])
remote_client.connect(
    host_ssh_config["hostname"],
    username=username,
    password="",
    sock=paramiko.ProxyCommand(host_ssh_config["proxycommand"]),
)

# Use the created client with Tamr-Toolbox functions
user_defined_configs = tbox.sysadmin.instance.get_configs(
    user_defined_only=True,
    tamr_install_dir="/home/ubuntu",
    remote_client=remote_client,
    impersonation_username="ubuntu",
)
print(user_defined_configs)
