from dataclasses import dataclass
from tamr_toolbox.models.data_type import JsonDict
from typing import List


@dataclass
class EmailInfo:
    """
    A dataclass to tie together relevant data to send an email.

    Args:
        sender_address: email address to send message from ex: my_pipeline@gmail.com
        sender_password: password to login to sender_email
        recipient_addresses: list of emails to send message to ex: [client_email@gmail.com]
        smtp_server: smtp server address of sender_email ex: smtp.gmail.com
        smtp_port: port to send email from, use 465 for SSL
    """

    sender_address: str
    sender_password: str
    recipient_addresses: List[str]
    smtp_server: str
    smtp_port: int


def from_config(*, config: JsonDict) -> EmailInfo:
    """Create an instance of EmailInfo from a json object.

    Args:
        config: A json dictionary containing email configuration values
    """
    return EmailInfo(
        sender_address=config["sender_address"],
        sender_password=config["sender_password"],
        recipient_addresses=config["recipient_addresses"],
        smtp_server=config["smtp_server"],
        smtp_port=config["smtp_port"],
    )
