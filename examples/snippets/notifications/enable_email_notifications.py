"""Example script for generating Email notifications based on Tamr jobs"""
import tamr_toolbox as tbox
from tamr_toolbox.models.operation_state import OperationState

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Email configuration object that include smtp server information
# and sender/recipeint email addresses
email_config = tbox.notifications.email_info.from_config(
    config={
        "sender_address": "sender@gmail.com",
        "sender_password": "sender_email_password",
        "recipient_addresses": ["recipient@gmail.com"],
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 465,
    }
)

# Send any email message
tbox.notifications.emails.send_email(
    message="This is a test message.", subject_line="Subject", email_config=email_config,
)

# Use case 1: Track the status updates for a specific job using its job id
tbox.notifications.emails.monitor_job(
    tamr=tamr, email_config=email_config, operation="my_job_id",
)

# Use case 2: Track the status updates for a job kicked off by the tamr-unify-client
project = tamr.projects.by_name("Project_1")
op = project.unified_dataset().refresh(asynchronous=True)
tbox.notifications.emails.monitor_job(
    tamr=tamr,
    email_config=email_config,
    operation=op,
    notify_states=[OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED],
)
