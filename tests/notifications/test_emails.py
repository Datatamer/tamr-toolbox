"""Tests for tasks related to creation of Email notifications"""
from email.mime.text import MIMEText
from unittest.mock import patch

import tamr_toolbox as tbox
from tamr_toolbox import utils
from tamr_toolbox.notifications.emails import EmailNotifier
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


@patch("smtplib.SMTP", autospec=True)
def test_tls_login(mock_smtp):
    _ = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        email_recipients=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True,
    )

    context = mock_smtp.return_value

    mock_smtp.assert_called()
    context.starttls.assert_called()
    context.login.assert_called()


@patch("smtplib.SMTP_SSL", autospec=True)
def test_non_tls_login(mock_smtp):
    _ = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        email_recipients=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=False,
    )

    context = mock_smtp.return_value

    mock_smtp.assert_called()
    context.starttls.assert_not_called()
    context.login.assert_called()


@patch("smtplib.SMTP", autospec=True)
def test_build_message(mock_smtp):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    expected_msg = MIMEText(test_message)
    expected_msg["Subject"] = "Tamr System: " + test_subject_line
    expected_msg["From"] = CONFIG["my_email_notification"]["sender_address"]
    expected_msg["To"] = CONFIG["my_email_notification"]["recipient_addresses"][0]
    expected_msg = expected_msg.as_string()

    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        email_recipients=CONFIG["my_email_notification"]["recipient_addresses"][0],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True,
    )

    message = notifier._build_message(test_message,
                                      test_subject_line,
                                      recipient=CONFIG["my_email_notification"]["recipient_addresses"][0])

    assert message == expected_msg


@patch("smtplib.SMTP", autospec=True)
def test_send_message(mock_smtp):
    test_message = "This is a test email."
    test_subject_line = "Test 123"

    expected_msg = MIMEText(test_message)
    expected_msg["Subject"] = "Tamr System: " + test_subject_line
    expected_msg["From"] = CONFIG["my_email_notification"]["sender_address"]
    expected_msg["To"] = CONFIG["my_email_notification"]["recipient_addresses"][1]
    expected_msg = expected_msg.as_string()

    notifier = EmailNotifier(
        sender_address=CONFIG["my_email_notification"]["sender_address"],
        sender_password=CONFIG["my_email_notification"]["sender_password"],
        email_recipients=CONFIG["my_email_notification"]["recipient_addresses"],
        smtp_server=CONFIG["my_email_notification"]["smtp_server"],
        smtp_port=CONFIG["my_email_notification"]["smtp_port"],
        use_tls=True,
    )

    notifier.send_message(test_message, test_subject_line)

    context = mock_smtp.return_value
    assert context.sendmail.call_count == 2
    assert context.sendmail.call_args_list[-1] == [{"from_addr":CONFIG["my_email_notification"]["sender_address"],
                                                    "to_addrs": CONFIG["my_email_notification"]["recipient_addresses"][1],
                                                    "msg": expected_msg}]


def test_deprecated_build_message():
    test_message = "This is a test email."
    subject_line = "Test 123"
    test_response = (
            'Content-Type: text/plain; charset="us-ascii"\nMIME-Version: '
            + "1.0\nContent-Transfer-Encoding: 7bit\n"
            + f'Subject: Test 123\nFrom: {CONFIG["my_email_notification"]["sender_address"]}\n'
            + f'To: {CONFIG["my_email_notification"]["recipient_addresses"][0]}, '
              f'{CONFIG["my_email_notification"]["recipient_addresses"][1]}\n'
            + "\nThis is a test email."
    )

    msg = tbox.notifications.emails._build_message(
        message=test_message,
        subject_line=subject_line,
        sender=CONFIG["my_email_notification"]["sender_address"],
        recipients=CONFIG["my_email_notification"]["recipient_addresses"],
    )

    assert msg == test_response


def test_deprecated_send_email_succeed():
    with patch("smtplib.SMTP_SSL", autospec=True) as mock_smtp:
        test_message = "This is a test email."
        subject_line = "Test"

        response = tbox.notifications.emails.send_email(
            message=test_message,
            subject_line=subject_line,
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["smtp_port"],
            use_tls=False,
        )

        # test smptp server was created
        mock_smtp.assert_called()

        # test login function was called
        context = mock_smtp.return_value.__enter__.return_value
        context.login.assert_called()

        # test smtplib sendmail function was called with correct parameters
        msg = tbox.notifications.emails._build_message(
            message=test_message,
            subject_line=subject_line,
            sender=CONFIG["my_email_notification"]["sender_address"],
            recipients=CONFIG["my_email_notification"]["recipient_addresses"],
        )
        context.sendmail.assert_called_with(
            CONFIG["my_email_notification"]["sender_address"],
            CONFIG["my_email_notification"]["recipient_addresses"],
            msg,
        )

        assert response["message"] == test_message


def test_deprecated_send_email_tls():
    with patch("smtplib.SMTP", autospec=True) as mock_smtp:
        test_message = "This is a test email."
        subject_line = "Test"

        tbox.notifications.emails.send_email(
            message=test_message,
            subject_line=subject_line,
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["tls_port"],
            use_tls=True,
        )

        # test smptp server was created
        mock_smtp.assert_called()

        # test login function was called
        context = mock_smtp.return_value.__enter__.return_value
        context.starttls.assert_called()
        context.login.assert_called()

        # test smtplib sendmail function was called with correct parameters
        msg = tbox.notifications.emails._build_message(
            message=test_message,
            subject_line=subject_line,
            sender=CONFIG["my_email_notification"]["sender_address"],
            recipients=CONFIG["my_email_notification"]["recipient_addresses"],
        )
        context.sendmail.assert_called_with(
            CONFIG["my_email_notification"]["sender_address"],
            CONFIG["my_email_notification"]["recipient_addresses"],
            msg,
        )


@mock_api()
def test_deprecated_monitor_job_succeed():
    with patch("smtplib.SMTP_SSL", autospec=True) as mock_smtp:
        client = utils.client.create(**CONFIG["my_instance_name"])
        project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
        op = project.unified_dataset().refresh(asynchronous=True)

        list_responses = tbox.notifications.emails.monitor_job(
            tamr=client,
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["smtp_port"],
            operation=op,
            poll_interval_seconds=0.01,
        )

        # test smptp server was created
        mock_smtp.assert_called()

        expected_messages = [
            (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
            ),
            (
                f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
                f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
                f"Status: RUNNING "
            ),
            (
                f"Host: {client.host} \n Job: {op.resource_id} \n Description: Materialize "
                f"views [minimal_schema_mapping_unified_dataset] to Elastic \n "
                f"Status: SUCCEEDED "
            ),
        ]

        # test that monitor job messages are expected
        messages = [resp["message"] for resp in list_responses]
        assert messages == expected_messages


@mock_api()
def test_deprecated_monitor_job_timeout():
    with patch("smtplib.SMTP_SSL", autospec=True) as mock_smtp:
        client = utils.client.create(**CONFIG["my_instance_name"])
        project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
        op = project.unified_dataset().refresh(asynchronous=True)

        timeout_seconds = 0.02
        list_responses = tbox.notifications.emails.monitor_job(
            tamr=client,
            sender_address=CONFIG["my_email_notification"]["sender_address"],
            sender_password=CONFIG["my_email_notification"]["sender_password"],
            recipient_addresses=CONFIG["my_email_notification"]["recipient_addresses"],
            smtp_server=CONFIG["my_email_notification"]["smtp_server"],
            smtp_port=CONFIG["my_email_notification"]["smtp_port"],
            operation=op,
            poll_interval_seconds=0.01,
            timeout_seconds=timeout_seconds,
        )

        # test smptp server was created
        mock_smtp.assert_called()

        expected_messages = [
            (
                f"Host: {client.host} \n Job: {op.resource_id} \n "
                f"Description: Materialize views "
                f"[minimal_schema_mapping_unified_dataset] to Elastic \n Status: PENDING "
            ),
            (
                f"The job {op.resource_id}: Materialize views "
                f"[minimal_schema_mapping_unified_dataset]"
                f" to Elastic took longer than {timeout_seconds} seconds to resolve."
            ),
        ]

        # test that monitor job messages are expected
        messages = [resp["message"] for resp in list_responses]
        assert messages == expected_messages
