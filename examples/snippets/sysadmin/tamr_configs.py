"""Getting and setting configuration for Tamr"""
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

# Get all user defined configs from a local instance as the current user
user_defined_configs = tbox.sysadmin.instance.get_configs(
    user_defined_only=True, tamr_install_dir="/data/tamr-home",
)
print(f"Use defined config: {user_defined_configs}")
# Example print output
"""Use defined config: {'TAMR_JOB_SPARK_EXECUTOR_CORES': 3,
'TAMR_LICENSE_KEY': 'd8mO2yX...prnfp2FL4='}"""

# Get a single config from a remote instance, using password authentication
tamr_log_dir = tbox.sysadmin.instance.get_config(
    config_name="TAMR_LOG_DIR",
    tamr_install_dir="/data/tamr-home",
    remote_client=remote_client,
    impersonation_username="tamr",
    impersonation_password=os.environ.get("TAMR_BASH_PASSWORD"),
)
print(f"Tamr logging directory: {tamr_log_dir}")
# Example print output
"""Tamr logging directory: /data/tamr-home/tamr/logs"""

# Get multiple config from a remote instance, using sudo for impersonation
es_config = tbox.sysadmin.instance.get_configs(
    config_names=["ES_HEAP_SIZE", "ES_NUM_SHARDS"],
    tamr_install_dir="/data/tamr-home",
    remote_client=remote_client,
    impersonation_username="tamr",
)
print(f"ElasticSearch Config: {es_config}")
# Example print output
"""ElasticSearch Config: {'ES_HEAP_SIZE': '11G', 'ES_NUM_SHARDS': 8}"""

# Get config based on a regular expression from a local instance, using sudo for impersonation
spark_config = tbox.sysadmin.instance.get_configs(
    config_search_regex="spark.*(mem|core|instances)",
    tamr_install_dir="/data/tamr-home",
    remote_client=remote_client,
    impersonation_username="tamr",
)
print(f"Spark config: {spark_config}")
# Example print output
"""Spark config: {'TAMR_JOB_SPARK_DRIVER_MEM': '3G', 'TAMR_JOB_SPARK_EXECUTOR_CORES': 3,
'TAMR_JOB_SPARK_EXECUTOR_INSTANCES': 1, 'TAMR_JOB_SPARK_EXECUTOR_MEM': '26G', '
TAMR_SPARK_CORES': 6, 'TAMR_SPARK_MEMORY': '33G'}"""


# Set multiple config values on a remote instance, using sudo for impersonation
changed_config = tbox.sysadmin.instance.set_configs(
    configs={"TAMR_SPARK_MEMORY": "50G", "ES_HEAP_SIZE": "5G"},
    tamr_install_dir="/data/tamr-home",
    remote_client=remote_client,
    impersonation_username="tamr",
)
# Note that the changed config returned includes indirect config changes caused by
# config formula calculations
print(f"Changed config: {changed_config}")
# Example print output
"""Changed config: {'ES_HEAP_SIZE': '5G', 'TAMR_JOB_SPARK_EXECUTOR_INSTANCES': 5,
'TAMR_JOB_SPARK_EXECUTOR_MEM': '8G', 'TAMR_SPARK_MEMORY': '50G'}"""

# After setting new configs, Tamr needs to be restarted for the new configs to take effect
tbox.sysadmin.instance.restart_tamr(
    tamr_install_dir="/data/tamr-home", remote_client=remote_client, impersonation_username="tamr"
)
