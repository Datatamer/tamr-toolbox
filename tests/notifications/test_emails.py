"""Tests for tasks related to creation of Email notifications"""
from unittest.mock import patch

from tamr_toolbox import utils
from tamr_toolbox.notifications.emails import EmailNotifier
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@patch("smtplib.SMTP", autospec=True)
def test_tls_login(mock_smtp):
    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True)

    context = mock_smtp.return_value

    mock_smtp.assert_called()
    context.starttls.assert_called()
    context.login.assert_called()


@patch("smtplib.SMTP_SSL", autospec=True)
def test_non_tls_login(mock_smtp):
    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=False)

    context = mock_smtp.return_value

    mock_smtp.assert_called()
    context.starttls.assert_not_called()
    context.login.assert_called()


@patch("smtplib.SMTP", autospec=True)
def test_build_message(mock_smtp):
    test_message = "This is a test email."
    test_subject_line = "Test 123"
    expected_msg = (
        'Content-Type: text/plain; charset="us-ascii"\nMIME-Version: '
        + "1.0\nContent-Transfer-Encoding: 7bit\n"
        + f'Subject: Tamr System: Test 123\nFrom: {CONFIG["my_email_notification"]["sender_address"]}\n'
        + f'To: {CONFIG["my_email_notification"]["recipient_addresses"][0]}\n'
        + "\nThis is a test email."
    )

    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True)

    msg = notifier._build_message(test_message, test_subject_line)

    assert msg == expected_msg


@patch("smtplib.SMTP", autospec=True)
def test_send_message(mock_smtp):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True)

    context = mock_smtp.return_value
    notifier.send_message(test_message, test_subject_line)
    context.sendmail.assert_called_with(
        CONFIG["my_email_notification"]["sender_address"],
        CONFIG["my_email_notification"]["recipient_addresses"],
        notifier._build_message(test_message, test_subject_line))
