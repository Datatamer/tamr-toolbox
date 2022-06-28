"""Tests for tasks related to creation of Email notifications"""
from unittest.mock import patch

import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.notifications.emails import EmailNotifier
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


def test_send_email_succeed():
    with patch("smtplib.SMTP_SSL", autospec=True) as mock_smtp:
        test_message = "This is a test email."
        subject_line = "Test"

        notifier = EmailNotifier(
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["smtp_port"],
            use_tls=False)

        # test smptp server was created
        mock_smtp.assert_called()

        # test login function was called
        context = mock_smtp.return_value.__enter__.return_value
        context.login.assert_called()

        notifier.send_message(message=test_message, title=subject_line)


def test_send_email_tls():
    with patch("smtplib.SMTP", autospec=True) as mock_smtp:
        test_message = "This is a test email."
        subject_line = "Test"

        notifier = EmailNotifier(
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["tls_port"],
            use_tls=True)

        notifier.send_message(message=test_message, title=subject_line)

        mock_smtp.assert_called()  # test smptp server was created

        context = mock_smtp.return_value.__enter__.return_value # test login function was called
        context.starttls.assert_called()
        context.login.assert_called()

        # test smtplib sendmail function was called with correct parameters
        context.sendmail.assert_called_with(
            CONFIG["my_email_notification"]["sender_address"],
            CONFIG["my_email_notification"]["recipient_addresses"],
            test_message,
        )
