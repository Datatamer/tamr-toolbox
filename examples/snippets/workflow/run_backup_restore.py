"""Example script for managing backup and restore operations for a Tamr instance"""
import tamr_toolbox as tbox

# Read config, make Tamr Client, make logger
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

LOGGER = tbox.utils.logger.create("my-script")

# List backups and create new backup
backups = tbox.workflow.backup.list_backups(tamr)
for tamr_backup in backups:
    LOGGER.debug(tamr_backup)

# optional: delete old sparkEventLogs before backup to reduce backup size
LOGGER.info("Deleting old sparkEventLogs")
tbox.workflow.backup.delete_old_spark_event_logs("/home/ubuntu", num_days_to_keep=14)

LOGGER.info("About to run backup")
op = tbox.workflow.backup.initiate_backup(tamr)
backup_id = op.json()["relativeId"]
state = op.json()["state"]
LOGGER.info(f"Completed backup with state {state} and relative ID {backup_id}")

# Restore to previous backup file
LOGGER.info("About to run restore to backup file")
backup_id = "1"  # update with the relativeID of your desired backup file
op = tbox.workflow.backup.initiate_restore(tamr, backup_id)
state = op.json()["state"]
LOGGER.info(f"Completed restore to backup file with ID {backup_id} with state {state}")

# Delete backups
result = tbox.workflow.backup.classify_backups("/path/to/my/backups/")
LOGGER.info(
    f"Backup directory /home/ubuntu/tamr/backups contains {result['succeeded']} successful backups"
    f"and {result['not_succeeded']} unsuccessful backups."
)
tbox.workflow.backup.delete_old_backups(
    "/path/to/my/backups/", num_successful_backups_to_keep=1, num_failed_backups_to_keep=1
)
LOGGER.info(
    f"Deleted {result['succeeded']-1} old successful backups and"
    f"{result['not_succeeded']-1} unsuccessful backups."
)
