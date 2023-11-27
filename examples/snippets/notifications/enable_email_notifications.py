"""Example script for generating Email notifications based on Tamr jobs"""
import tamr_toolbox as tbox
from tamr_toolbox.models.operation_state import OperationState

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Make Email configuration object that include smtp server information
# and sender/recipient email addresses
config = {
    "sender_address": "sender@gmail.com",
    "sender_password": "sender_email_password",
    "recipient_addresses": ["recipient@gmail.com"],
    "cc_addresses": ["cc_address@example.com", "another_cc@gmail.com"],
    "bcc_addresses": ["bcc_address@example.com", "another_bcc@gmail.com"],
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
}


# Send any email message
tbox.notifications.emails.send_email(
    message="This is a test message.",
    subject_line="Subject",
    sender_address=config["sender_address"],
    sender_password=config["sender_password"],
    recipient_addresses=config["recipient_addresses"],
    smtp_server=config["smtp_server"],
    smtp_port=config["smtp_port"],
)

# Use case 1: Track the status updates for a specific job using its job id
tbox.notifications.emails.monitor_job(
    tamr=tamr,
    sender_address=config["sender_address"],
    sender_password=config["sender_password"],
    recipient_addresses=config["recipient_addresses"],
    smtp_server=config["smtp_server"],
    smtp_port=config["smtp_port"],
    operation="my_job_id",
)

# Use case 2: Track the status updates for a job kicked off by the tamr-unify-client
project = tamr.projects.by_name("Project_1")
op = project.unified_dataset().refresh(asynchronous=True)
tbox.notifications.emails.monitor_job(
    tamr=tamr,
    sender_address=config["sender_address"],
    sender_password=config["sender_password"],
    recipient_addresses=config["recipient_addresses"],
    smtp_server=config["smtp_server"],
    smtp_port=config["smtp_port"],
    operation=op,
    notify_states=[OperationState.SUCCEEDED, OperationState.FAILED, OperationState.CANCELED],
)

# Use case 3: Send status updates for a specific job with CC and BCC recipients
tbox.notifications.emails.monitor_job(
    tamr=tamr,
    sender_address=config["sender_address"],
    sender_password=config["sender_password"],
    recipient_addresses=config["recipient_addresses"],
    cc_addresses=config["cc_addresses"],
    bcc_addresses=config["bcc_addresses"],
    smtp_server=config["smtp_server"],
    smtp_port=config["smtp_port"],
    operation="my_job_id",
)
