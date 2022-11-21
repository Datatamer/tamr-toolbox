"""Example script for generating Slack notifications based on Tamr jobs"""
import tamr_toolbox as tbox
from slack import WebClient

from tamr_toolbox.models.operation_state import OperationState

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Slack Client with your own slack api token, a Bot User token that starts with "xoxb-"
slack_api_token = "xoxb-12345-12345-A1b2C3d4E5"
slack_client = WebClient(token=slack_api_token)

# Send any texts to a Slack channel
tbox.notifications.slack.send_message(
    slack_client=slack_client, channel="#test_tbox_messaging", message="This is a test message."
)

# Use case 1: Track the status updates for a specific job using its job id
tbox.notifications.slack.monitor_job(
    tamr=tamr, slack_client=slack_client, channel="#test_tbox_messaging", operation="my_job_id"
)

# Use case 2: Track the status updates for a job kicked off by the tamr-unify-client
project = tamr.projects.by_name("Project_1")
op = project.unified_dataset().refresh(asynchronous=True)
tbox.notifications.slack.monitor_job(
    tamr=tamr,
    slack_client=slack_client,
    channel="#test_tbox_messaging",
    operation=op,
    notify_states=[OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED],
)
