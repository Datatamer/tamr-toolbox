import logging
import smtplib
import ssl
from abc import ABC
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import List, Optional

from tamr_toolbox.notifications.core import _BaseNotifier

LOGGER = logging.getLogger(__name__)


@dataclass
class EmailNotifier(_BaseNotifier, ABC):
    recipient_addresses: List[str]
    sender_address: str
    sender_password: str
    smtp_server: str
    smtp_port: int = 465
    use_tls: bool = False
    keyfile: Optional[str] = None
    certfile: Optional[str] = None
    """
    Send emails based on Tamr eventing.
    
    Attributes:
        sender_address: Email address to send messages from, such as my_pipeline@gmail.com
        sender_password: Password for sending email address 
        recipient_addresses: List of emails to send messages to
        smtp_server: Outbound smtp server address
        smtp_port: Port to send email from, such as 465 for SSL (default)
        use_tls: If True, use TLS protocol. False by default.
        keyfile: Path to a TLS/SSL private key, (typically .pem). Default is None.
        certfile: TLS/SSL cert file issued by a Certificate Authority (CA), usually PEM format. 
            Default is None
        
    Examples:
    
    Raises:
        SMTPException: If email server login fails
    """

    def __post_init__(self):
        self.server = None
        self._setup_server()

    def _setup_server(self):
        context = ssl.create_default_context()
        if self.use_tls:
            self.server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.server.starttls(keyfile=self.keyfile, certfile=self.certfile, context=context)
        else:
            self.server = smtplib.SMTP_SSL(
                self.smtp_server, self.smtp_port, keyfile=self.keyfile, certfile=self.certfile, context=context)

        LOGGER.info("Trying to connect to the email server")
        self.server.login(self.sender_address, self.sender_password)
        LOGGER.info("Logged in to the email server successfully")

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        msg = MIMEText(message)
        msg["Subject"] = "Tamr System: " + title
        msg["From"] = self.sender_address
        msg["To"] = ", ".join(self.recipient_addresses)

        LOGGER.info(f"Sending an email with payload {msg.as_string()}")
        response = self.server.sendmail(self.sender_address, self.recipient_addresses, msg.as_string())
        if response:
            LOGGER.info("Email sent successfully")
        else:
            LOGGER.error(f"Email failed to send, with reason {response}")
