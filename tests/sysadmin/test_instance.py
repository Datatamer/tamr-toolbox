"""Test for tasks related to a Tamr instance"""
from unittest import mock
from unittest.mock import MagicMock, call

import paramiko
import pytest
import platform

import tamr_toolbox as tbox


def test__run_local_command():
    # Test successful command
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_local_command('echo "Hello World"')
    assert exit_code == 0
    assert "Hello World" in stdout
    assert len(stderr) == 0

    # Test unsuccessful command
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_local_command("thisIsNotACommand")
    assert exit_code != 0
    assert len(stdout) == 0
    assert "not found" in stderr or "not recognized" in stderr

    # Test command with input
    if platform.system() == "Windows":
        # Environment variables cannot be set and read in a single line on windows
        # So we complete the input test with two commands
        exit_code, stdout, stderr = tbox.sysadmin.instance._run_local_command(
            'set /p uservar=""', command_input=b"my_name\n"
        )
        assert exit_code == 0

        exit_code, stdout, stderr = tbox.sysadmin.instance._run_local_command(
            "echo Hello %uservar%"
        )
        assert exit_code == 0
        assert "Hello my_name" in stdout
        assert len(stderr) == 0

    else:
        exit_code, stdout, stderr = tbox.sysadmin.instance._run_local_command(
            'read uservar && echo "Hello $uservar"', command_input=b"my_name\n"
        )
        assert exit_code == 0
        assert "Hello my_name" in stdout
        assert len(stderr) == 0


def test__run_remote_command():
    # Mock out a remote client, ensure that the the right arguments are passed to the remote client
    remote_client = paramiko.SSHClient()
    channel = paramiko.channel.Channel(0)
    transport = paramiko.transport.Transport
    stdin_file = channel.makefile("stdin")
    stdin_file.write = MagicMock()
    stdout_file = channel.makefile("stdout")
    stderr_file = channel.makefile("stderr")
    remote_client.exec_command = MagicMock(return_value=(stdin_file, stdout_file, stderr_file))
    remote_client.get_transport = MagicMock(return_value=transport)
    transport.getpeername = MagicMock(return_value=["0.0.0.0"])
    transport.get_username = MagicMock(return_value="my_username")
    channel.exit_status_ready = MagicMock(return_value=True)
    basic_command = 'echo "Hello World"'

    tbox.sysadmin.instance._run_remote_command(
        basic_command, remote_client=remote_client, command_input=b"test"
    )
    remote_client.exec_command.assert_called_with(basic_command)
    stdin_file.write.assert_called_with(b"test")

    # Make sure files are closed
    stdin_file.close()
    stdout_file.close()
    stderr_file.close()


@mock.patch("tamr_toolbox.sysadmin.instance._run_local_command", return_value=(0, "out", "err"))
@mock.patch("tamr_toolbox.sysadmin.instance._run_remote_command", return_value=(0, "", ""))
def test__run_command(run_remote, run_local):
    basic_command = 'echo "Hello World"'

    # Test that defaults lead to a simple command run locally
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_command(
        basic_command, enforce_success=True
    )
    assert exit_code == 0
    assert stdout == "out"
    assert stderr == "err"
    run_local.assert_called_with(basic_command, command_input=None)

    # Test that including a remote client leads to sending the command as remote
    remote_client = paramiko.SSHClient()
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_command(
        basic_command, remote_client=remote_client
    )
    assert exit_code == 0
    run_remote.assert_called_with(basic_command, remote_client=remote_client, command_input=None)

    # Test impersonation without password
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_command(
        basic_command, impersonation_username="my_user"
    )
    assert exit_code == 0
    run_local.assert_called_with(f"sudo su - my_user -c '{basic_command}'", command_input=None)

    # Test impersonation with password
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_command(
        basic_command, impersonation_username="my_user", impersonation_password="my_pass"
    )
    assert exit_code == 0
    run_local.assert_called_with(f"su - my_user -c '{basic_command}'", command_input=b"my_pass")


@mock.patch("tamr_toolbox.sysadmin.instance._run_local_command", return_value=(1, "", ""))
def test__run_command_with_failure(run_local):
    basic_command = 'echo "Hello World"'

    # Test enforce success
    with pytest.raises(RuntimeError):
        tbox.sysadmin.instance._run_command(basic_command, enforce_success=True)
    exit_code, stdout, stderr = tbox.sysadmin.instance._run_command(
        basic_command, enforce_success=False
    )
    assert exit_code == 1


@mock.patch("tamr_toolbox.sysadmin.instance._run_command", return_value=(0, "", ""))
def test_start_tamr(run_command):
    tamr_dir = "/data/tamr-home"

    # Test starting with dependencies
    tbox.sysadmin.instance.start_tamr(tamr_install_dir=tamr_dir, include_dependencies=True)
    run_command.has_calls(
        [
            call(
                command=f"{tamr_dir}/tamr/start-dependencies.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/start-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
        ]
    )

    # Test starting without dependencies
    run_command.reset_mock()
    tbox.sysadmin.instance.start_tamr(tamr_install_dir=tamr_dir, include_dependencies=False)
    run_command.assert_called_once_with(
        command=f"{tamr_dir}/tamr/start-unify.sh",
        remote_client=None,
        impersonation_username=None,
        impersonation_password=None,
        enforce_success=True,
    )


@mock.patch("tamr_toolbox.sysadmin.instance._run_command", return_value=(0, "", ""))
def test_stop_tamr(run_command):
    tamr_dir = "/data/tamr-home"

    # Test stopping with dependencies
    tbox.sysadmin.instance.stop_tamr(tamr_install_dir=tamr_dir, include_dependencies=True)
    run_command.has_calls(
        [
            call(
                command=f"{tamr_dir}/tamr/stop-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/stop-dependencies.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
        ]
    )

    # Test stopping without dependencies
    run_command.reset_mock()
    tbox.sysadmin.instance.stop_tamr(tamr_install_dir=tamr_dir, include_dependencies=False)
    run_command.assert_called_once_with(
        command=f"{tamr_dir}/tamr/stop-unify.sh",
        remote_client=None,
        impersonation_username=None,
        impersonation_password=None,
        enforce_success=True,
    )


@mock.patch("tamr_toolbox.sysadmin.instance._run_command", return_value=(0, "", ""))
def test_restart_tamr(run_command):
    tamr_dir = "/data/tamr-home"

    # Test restarting with dependencies
    tbox.sysadmin.instance.restart_tamr(tamr_install_dir=tamr_dir, include_dependencies=True)
    run_command.has_calls(
        [
            call(
                command=f"{tamr_dir}/tamr/stop-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/stop-dependencies.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/start-dependencies.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/start-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
        ]
    )

    # Test restarting without dependencies
    run_command.reset_mock()
    tbox.sysadmin.instance.restart_tamr(tamr_install_dir=tamr_dir, include_dependencies=False)
    run_command.has_calls(
        [
            call(
                command=f"{tamr_dir}/tamr/stop-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
            call(
                command=f"{tamr_dir}/tamr/start-unify.sh",
                remote_client=None,
                impersonation_username=None,
                impersonation_password=None,
                enforce_success=True,
            ),
        ]
    )
