"""Starting and stopping Tamr"""
import tamr_toolbox as tbox
import paramiko
import os


# Activate logging
tbox.utils.logger.enable_toolbox_logging()

# Create a paramiko client for remotely connecting to a Tamr instance
remote_client = paramiko.SSHClient()
remote_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
remote_client.connect(
    hostname="10.9.999.999", username="ubuntu", key_filename="/full/path/to/my-key.pem"
)

# Restart a remote instance, using sudo for impersonation, with verbose logging
tbox.sysadmin.instance.restart_tamr(
    tamr_install_dir="/data/tamr-home",
    remote_client=remote_client,
    impersonation_username="tamr",
    verbose=True,
)

# Stop a local instance as the current user
tbox.sysadmin.instance.stop_tamr(tamr_install_dir="/data/tamr-home/")

# Start a local instance as another user using password authentication
tbox.sysadmin.instance.start_tamr(
    tamr_install_dir="/data/tamr-home/",
    impersonation_username="tamr",
    impersonation_password=os.environ.get("TAMR_BASH_PASSWORD"),
)
