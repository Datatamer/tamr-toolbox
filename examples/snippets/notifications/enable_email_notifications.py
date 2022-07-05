"""Example script for generating Email notifications based on Tamr jobs"""
import tamr_toolbox as tbox
from tamr_toolbox.models.operation_state import OperationState

# Make Email configuration object that include smtp server information
# and sender/recipient email addresses
config = {
    "sender_address": "sender@gmail.com",
    "sender_password": "sender_email_password",
    "recipient_addresses": ["recipient@gmail.com", "recipient2@gmail.com"],
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
}

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Email notifier
notifier = tbox.notifications.emails.EmailNotifier(
    sender_address=config["sender_address"],
    sender_password=config["sender_password"],
    recipient_addresses=config["recipient_addresses"],
    smtp_server=config["smtp_server"],
    smtp_port=config["smtp_port"],
)

# Example 1: Send any email message
notifier.send_message(
    message="This is a test message.",
    title="Subject",
)

# Example 2: Track the status updates for a specific job using its job id
notifier.monitor_job(
    tamr=tamr,
    operation="my_job_id",
)

# Example 3: Track the status updates for a job kicked off by the tamr-unify-client
project = tamr.projects.by_name("Project_1")
op = project.unified_dataset().refresh(asynchronous=True)
notifier.monitor_job(
    tamr=tamr,
    operation=op,
    notify_states=[OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED],
)
