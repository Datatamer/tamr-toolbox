"""Tasks related to a Tamr instance"""

from typing import Optional
import paramiko
import subprocess
import logging
import time

LOGGER = logging.getLogger(__name__)


def _run_remote_command(
    command: str, *, remote_client: paramiko.SSHClient, command_input: Optional[bytes] = None
) -> (int, str, str):
    """Runs the provided command in a remote environment using the provided ssh client

        Args:
            command: The command to run
            remote_client: An ssh client providing a remote connection
            command_input: Content to send to stdin after command is started

        Returns:
            (exit code of command, stdout of command, stderr of command)

        """

    # Initiate command
    stdin_file, stdout_file, stderr_file = remote_client.exec_command(command)
    command_channel = stdout_file.channel

    # Provide input to command (such as a password) if given
    if command_input is not None:
        stdin_file.write(command_input)
    stdin_file.close()

    # Wait for command to complete
    while not command_channel.exit_status_ready():
        time.sleep(1)

    # Collect command output
    # Some bash scripts do not close their stderr and stdout even after the exit status is ready
    # This leads to calls like stdout_file.readlines() to hang indefinitely
    # We empty the buffer instead to avoid this issue
    stderr = command_channel.in_stderr_buffer.empty().decode("utf-8")
    stdout = command_channel.in_buffer.empty().decode("utf-8")
    stdout_file.close()
    stderr_file.close()

    return command_channel.exit_status, stdout, stderr


def _run_local_command(command: str, *, command_input: Optional[bytes] = None) -> (int, str, str):
    """Runs the provided command in the local shell

    Args:
        command: The command to run
        command_input: Content to send to stdin after command is started

    Returns:
        (exit code of command, stdout of command, stderr of command)

    """

    # Initiate command
    process = subprocess.Popen(
        command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE, shell=True
    )

    # Provide input to command (such as a password) if given
    if command_input is not None:
        process.stdin.write(command_input)
    process.stdin.close()

    # Wait for command to complete
    while process.poll() is None:
        time.sleep(1)

    # Collect command output
    # Some bash scripts do not close their stderr and stdout even after the exit status is ready
    # This leads to calls like stdout.readlines() to hang indefinitely
    # We use peek instead to avoid this issue
    stdout = process.stdout.peek().decode("utf-8")
    stderr = process.stderr.peek().decode("utf-8")

    return process.returncode, stdout, stderr


def _run_command(
    command: str,
    *,
    remote_client: Optional[paramiko.SSHClient] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
    enforce_success: bool = True,
) -> (int, str, str):
    """Runs the provided command in a remote environment if an ssh client is specified otherwise
    run the provided command in the local shell

    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used

    Args:
        command: The command to run
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as
        impersonation_password: The password for the impersonation_username
        enforce_success: Whether to throw an error if the command fails

    Returns:
        (exit code of command, stdout of command, stderr of command)

    Raises:
        RuntimeError: Raised when enforce_success is True and the command exists with an non-0 code

    """
    # If a username is specified, switch users for running the command
    if impersonation_username is not None:
        if impersonation_password is None:
            # If no password is provided use sudo
            command = f"sudo su - {impersonation_username} -c '{command}'"
            password_input = None
        else:
            # If a password is required pass it into the running process
            command = f"su - {impersonation_username} -c '{command}'"
            password_input = impersonation_password.encode("utf-8")
    else:
        # If no username is specified, no password is needed
        password_input = None

    # Run the command on the local or remote system
    if remote_client is None:
        exit_code, stdout, stderr = _run_local_command(command, command_input=password_input)
    else:
        exit_code, stdout, stderr = _run_remote_command(
            command, remote_client=remote_client, command_input=password_input
        )

    # When enforce_success is True, raise an error for non-zero exit codes
    if enforce_success and exit_code != 0:
        if remote_client is None:
            task_description = f"local command. "
        else:
            remote_ip = remote_client.get_transport().getpeername[0]
            remote_username = remote_client.get_transport().get_username()
            task_description = f"Failed to run remote command on {remote_ip} as {remote_username}."
        raise RuntimeError(
            f"{task_description}"
            f"Command: [{command}] exited with code {exit_code}.\n"
            f"STDOUT: [{stdout.strip()}]\n"
            f"STDERR: [{stderr.strip()}]"
        )
    return exit_code, stdout, stderr


def start_tamr(
    *,
    tamr_install_dir: str,
    include_dependencies: bool = True,
    remote_client: Optional[paramiko.SSHClient] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
) -> None:
    """Starts the Tamr software and the Tamr dependencies if include_dependencies is true.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.

    Args:
        tamr_install_dir: Full path to directory where Tamr is installed
        include_dependencies: Whether Tamr dependencies should be started
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
        None

    Raises:
        RuntimeError: Raised if Tamr start script(s) fail

    """
    if include_dependencies:
        _run_command(
            command=f"{tamr_install_dir}/tamr/start-dependencies.sh",
            remote_client=remote_client,
            impersonation_username=impersonation_username,
            impersonation_password=impersonation_password,
            enforce_success=True,
        )
    _run_command(
        command=f"{tamr_install_dir}/tamr/start-unify.sh",
        remote_client=remote_client,
        impersonation_username=impersonation_username,
        impersonation_password=impersonation_password,
        enforce_success=True,
    )


def stop_tamr(
    *,
    tamr_install_dir: str,
    include_dependencies: bool = True,
    remote_client: Optional[paramiko.SSHClient] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
) -> None:
    """Stops the Tamr software and the Tamr dependencies if include_dependencies is true.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.

    Args:
        tamr_install_dir: Full path to directory where Tamr is installed
        include_dependencies: Whether Tamr dependencies should be stopped
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
        None

    Raises:
        RuntimeError: Raised if Tamr stop script(s) fail

    """
    _run_command(
        command=f"{tamr_install_dir}/tamr/stop-unify.sh",
        remote_client=remote_client,
        impersonation_username=impersonation_username,
        impersonation_password=impersonation_password,
        enforce_success=True,
    )
    if include_dependencies:
        _run_command(
            command=f"{tamr_install_dir}/tamr/stop-dependencies.sh",
            remote_client=remote_client,
            impersonation_username=impersonation_username,
            impersonation_password=impersonation_password,
            enforce_success=True,
        )


def restart_tamr(
    *,
    tamr_install_dir: str,
    include_dependencies: bool = True,
    remote_client: Optional[paramiko.SSHClient] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
) -> None:
    """Restarts the Tamr software and the Tamr dependencies if include_dependencies is true.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.

    Args:
        tamr_install_dir: Full path to directory where Tamr is installed
        include_dependencies: Whether Tamr dependencies should be restarted
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
        None

    Raises:
        RuntimeError: Raised if Tamr start or stop script(s) fail

    """
    stop_tamr(
        tamr_install_dir=tamr_install_dir,
        include_dependencies=include_dependencies,
        remote_client=remote_client,
        impersonation_username=impersonation_username,
        impersonation_password=impersonation_password,
    )
    start_tamr(
        tamr_install_dir=tamr_install_dir,
        include_dependencies=include_dependencies,
        remote_client=remote_client,
        impersonation_username=impersonation_username,
        impersonation_password=impersonation_password,
    )
