"""Tasks related to creation of Email notifications"""
import logging
import time
import smtplib
import ssl

from typing import Union, List, Optional
from email.mime.text import MIMEText
from smtplib import SMTPException

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import get_details, from_resource_id

LOGGER = logging.getLogger(__name__)


def _build_message(
    *, message: str, subject_line: str, sender: str, recipients: List[str]
) -> MIMEText:
    """Builds email message in Multipurpose Internet Mail Extensions (MIME) format. MIME is an
    Internet standard that extends the format of email messages

    Args:
        message: Body of email message
        subject_line: subject of email
        sender: email address of sender
        recipients: list of emails to send message to

    Returns:
        Email as a string
    """

    # build email
    msg = MIMEText(message)
    msg["Subject"] = subject_line
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    return msg.as_string()


def send_email(
    *,
    message: str,
    subject_line: str,
    sender_address=str,
    sender_password=str,
    recipient_addresses=List[str],
    smtp_server=str,
    smtp_port=str,
    raise_error: bool = True,
) -> dict:
    """Sends a message via email to list of recipients

    Args:
        message: Body of email message
        subject_line: subject of email
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL
        raise_error: A boolean value to opt out raising SMTP errors

    Returns:
        The response codes from the smtp server for each email if there are any errors

    Raises:
        A dictionary, with one entry for each
        recipient that was refused.  Each entry contains a tuple of the SMTP
        error code and the accompanying error message sent by the server.
    """
    # build email
    msg = _build_message(
        message=message,
        subject_line=subject_line,
        sender=sender_address,
        recipients=recipient_addresses,
    )
    response = None

    try:
        context = ssl.create_default_context()
        # send message
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(sender_address, sender_password)
            response = server.sendmail(sender_address, recipient_addresses, msg)
    except SMTPException as e:
        LOGGER.error(f"Error: {e}")

        if not raise_error:
            response = {
                "type": "SMTPException",
                "text": f'The email: "{message}" failed to send.',
                "error": e,
            }
        else:
            raise e

    return response


def _send_job_status_message(
    *,
    sender_address=str,
    sender_password=str,
    recipient_addresses=List[str],
    smtp_server=str,
    smtp_port=str,
    operation: Operation,
    notify_states: List[OperationState],
) -> dict:
    """
    Checks operation state and if in `notify_states` sends the message.

    Args:
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL
        operation: A Tamr Operation
        notify_states: States for which notifications should be sent

    Returns:
        A dictionary, with one entry for each
        recipient that was refused.  Each entry contains a tuple of the SMTP
        error code and the accompanying error message sent by the server.
    """
    state = OperationState[operation.state]
    message, resp = None, None
    if state in notify_states:
        message = get_details(operation=operation)
        resp = send_email(
            message=message,
            subject_line=f"Job {operation.resource_id}: {state}",
            sender_address=sender_address,
            sender_password=sender_password,
            recipient_addresses=recipient_addresses,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
        )
    return (message, resp)


def monitor_job(
    tamr: Client,
    *,
    sender_address=str,
    sender_password=str,
    recipient_addresses=List[str],
    smtp_server=str,
    smtp_port=str,
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
) -> List[dict]:
    """

    Monitors a Tamr Operation and sends an email when the job status is updated

    Args:
        tamr: A Tamr client
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL
        operation: A job ID or a Tamr operation
        poll_interval_seconds: Time interval (in seconds) between subsequent polls
        timeout_seconds: Time (in seconds) to wait
        notify_states : States for which notifications should be sent, use None for all states

    Returns:
        A list of messages with their response codes
    """
    if notify_states is None:
        notify_states = [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED,
            OperationState.PENDING,
            OperationState.RUNNING,
        ]
    started = time.time()
    list_responses = []
    status = None

    if isinstance(operation, Operation):
        op = operation
    else:
        op = from_resource_id(tamr=tamr, job_id=operation)

    while (timeout_seconds is None or time.time() - started < timeout_seconds) and status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        # Check the status of the current operation.
        # If the state is updated such that status!=new_status then send a message and
        # update `status`.
        # The loop exits if hits the timeout or if the jobs is in
        # a final state (SUCCEEDED/FAILED/CANCELED).
        # Note that it will always send a message about any status that has been
        # updated before exiting
        op = op.poll()
        new_status = OperationState[op.state]
        if status != new_status:
            message, resp = _send_job_status_message(
                sender_address=sender_address,
                sender_password=sender_password,
                recipient_addresses=recipient_addresses,
                smtp_server=smtp_server,
                smtp_port=smtp_port,
                operation=op,
                notify_states=notify_states,
            )
            list_responses.append((message, resp))
            status = new_status
        time.sleep(poll_interval_seconds)

    if status not in [
        OperationState.SUCCEEDED,
        OperationState.FAILED,
        OperationState.CANCELED,
    ]:
        # If the operation was not in a final state then assume it timed out
        timeout_message = (
            f"The job {op.resource_id}: {op.description} took longer "
            f"than {timeout_seconds} seconds to resolve."
        )
        resp = send_email(
            message=timeout_message,
            subject_line=f"Job {op.resource_id}: Timeout",
            sender_address=sender_address,
            sender_password=sender_password,
            recipient_addresses=recipient_addresses,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
        )
        list_responses.append((timeout_message, resp))

    return list_responses
