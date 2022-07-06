"""Example script for generating Slack notifications based on Tamr jobs"""
import tamr_toolbox as tbox

from tamr_toolbox.models.operation_state import OperationState

# Make Slack config object with your own slack api token, a Bot User that starts with "xoxb-"
config = {"slack_api_token": "xoxb-12345-12345-A1b2C3d4E5", "slack_channel": "#my_slack_channel"}

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Email notifier
notifier = tbox.notifications.slack.SlackNotifier(
    channels=config["slack_channel"], token=config["slack_api_token"]
)

# Example 1: Send any slack message
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
