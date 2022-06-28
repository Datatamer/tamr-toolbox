"""Tasks related to creation of Email notifications"""
import logging
import smtplib
import ssl

from typing import Union, List, Optional, Tuple
from tamr_toolbox.models.data_type import JsonDict
from email.mime.text import MIMEText
from tamr_toolbox.notifications.common import _monitor_job as monitor_job_common

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import get_details

LOGGER = logging.getLogger(__name__)


def _build_message(*, message: str, subject_line: str, sender: str, recipients: List[str]) -> str:
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
    sender_address: str,
    sender_password: str,
    recipient_addresses: List[str],
    smtp_server: str,
    smtp_port: str,
    use_tls: bool = True,
    keyfile: Optional[str] = None,
    certfile: Optional[str] = None,
) -> JsonDict:
    """Sends a message via email to list of recipients

    Args:
        message: Body of email message
        subject_line: subject of email
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL, use 587 for TLS
        use_tls: A boolean value to turn on/off TLS protocol
        keyfile: the private key to a TLS/SSL certificate, usually PEM format
        certfile: TLS/SSL cert file issued by a Certificate Authority (CA), usually PEM format

    Returns:
        A dict with the message and response codes from the smtp server if there are any
        errors. The dictionary will contain one entry for each recipient that was refused.
        Each entry contains a tuple of the SMTP error code and the accompanying error
        message sent by the server. A successful response will contain an empty dict.

    Raises:
        SMTPException. The base exception class used by the smtplib module
    """
    # build email
    msg = _build_message(
        message=message,
        subject_line=subject_line,
        sender=sender_address,
        recipients=recipient_addresses,
    )
    response = dict()
    response["message"] = message

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, smtp_port) if use_tls else smtplib.SMTP_SSL(
        smtp_server, smtp_port, keyfile=keyfile, certfile=certfile, context=context
    ) as server:
        if use_tls:
            server.starttls(keyfile=keyfile, certfile=certfile, context=context)

        # login and send message
        server.login(sender_address, sender_password)
        errors = server.sendmail(sender_address, recipient_addresses, msg)
        response["errors"] = errors

    return response


def _send_job_status_message(
    *,
    sender_address: str,
    sender_password: str,
    recipient_addresses: List[str],
    smtp_server: str,
    smtp_port: str,
    operation: Operation,
    notify_states: List[OperationState],
    use_tls: bool = False,
    keyfile: Optional[str] = None,
    certfile: Optional[str] = None,
) -> JsonDict:
    """Checks operation state and if in `notify_states` sends the message.

    Args:
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL
        operation: A Tamr Operation
        notify_states: States for which notifications should be sent
        use_tls: A boolean value to opt to use TLS protocol
        keyfile: the private key to a TLS/SSL certificate, usually PEM format
        certfile: TLS/SSL cert file issued by a Certificate Authority (CA), usually PEM format

    Returns:
        A dict with the message and response codes from the smtp server if there are any
        errors. The dictionary will contain one entry for each recipient that was refused.
        Each entry contains a tuple of the SMTP error code and the accompanying error
        message sent by the server. A successful response will contain an empty dict.
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
            use_tls=use_tls,
            keyfile=keyfile,
            certfile=certfile,
        )
    return resp


def monitor_job(
    tamr: Client,
    *,
    sender_address: str,
    sender_password: str,
    recipient_addresses: List[str],
    smtp_server: str,
    smtp_port: str,
    operation: Union[int, str, Operation],
    poll_interval_seconds: float = 1,
    timeout_seconds: Optional[float] = None,
    notify_states: Optional[List[OperationState]] = None,
    use_tls: bool = False,
    keyfile: Optional[str] = None,
    certfile: Optional[str] = None,
) -> List[Tuple[str, JsonDict]]:
    """Monitors a Tamr Operation and sends an email when the job status is updated

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
        use_tls: A boolean value to opt to use TLS protocol
        keyfile: the private key to a TLS/SSL certificate, usually PEM format
        certfile: TLS/SSL cert file issued by a Certificate Authority (CA), usually PEM format

    Returns:
        A list of dicts. Each dict comtains the message and error response codes from the smtp
        server. The dictionary will contain one entry for each recipient that was refused.
        Each entry contains a tuple of the SMTP error code and the accompanying error
        message sent by the server. A successful response will contain an empty dict.
    """
    list_responses = monitor_job_common(
        tamr=tamr,
        send_message=send_email,
        send_status_function=_send_job_status_message,
        sender_address=sender_address,
        sender_password=sender_password,
        recipient_addresses=recipient_addresses,
        smtp_server=smtp_server,
        smtp_port=smtp_port,
        operation=operation,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
        notify_states=notify_states,
        use_tls=use_tls,
        keyfile=keyfile,
        certfile=certfile,
    )

    return list_responses
