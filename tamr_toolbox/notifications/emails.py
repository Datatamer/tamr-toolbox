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
    smtp_port: int
    use_tls: Optional[bool] = False
    keyfile: Optional[str] = None
    certfile: Optional[str] = None
    """
    Send emails based on Tamr eventing.
    
    Attributes:
        recipient_addresses: List of emails to send messages to
        sender_address: Email address to send messages from, such as my_pipeline@gmail.com
        sender_password: Password for sending email address 
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
        super().__init__()
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

    def _build_message(self, message: str, title: str):
        msg = MIMEText(message)
        msg["Subject"] = "Tamr System: " + title
        msg["From"] = self.sender_address
        msg["To"] = ", ".join(self.recipient_addresses)
        return msg.as_string()

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        msg = self._build_message(message, title)

        LOGGER.info(f"Sending an email to {self.recipient_addresses} with payload {msg}")
        self.server.sendmail(self.sender_address, self.recipient_addresses, msg)
        LOGGER.info("Email sent successfully")

        self.sent_messages += [msg]
