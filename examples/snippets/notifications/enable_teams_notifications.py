"""Example script for generating Slack notifications based on Tamr jobs"""
import tamr_toolbox as tbox

from tamr_toolbox.models.operation_state import OperationState

# Make Slack config object with your own slack api token, a Bot User that starts with "xoxb-"
config = {"webhooks": ["1234512345A1b2C3d4E5", "1234512345A1b2C3d4E6"]}

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Teams notifier
notifier = tbox.notifications.teams.TeamsNotifier(webhooks=config["webhooks"])

# Example 1: Send any Teams message
notifier.send_message(message="This is a test message.", title="Subject")

# Example 2: Track the status updates for a specific job using its job id
notifier.monitor_job(tamr=tamr, operation="my_job_id")

# Example 3: Track the status updates for a job kicked off by the tamr-unify-client
project = tamr.projects.by_name("Project_1")
op = project.unified_dataset().refresh(asynchronous=True)
notifier.monitor_job(
    tamr=tamr,
    operation=op,
    notify_states=[OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED],
)
