"Helper functions related to creating & managing custom UI buttons as yaml files"

import logging
from typing import List, Optional
import yaml
import os

from tamr_toolbox.sysadmin.instance import _run_command

LOGGER = logging.getLogger(__name__)


def create_redirect_button(
    *,
    button_id: str,
    button_text: str,
    page_names: List[str],
    redirect_url: str,
    open_in_new_tab: bool,
    output_dir: str,
    button_name: str,
) -> dict:
    """Create yaml file with all required attributes for
    a 'REDIRECT' UI button

    Args:
        button_id: A short identifier for the button to use in the,
                   body of a POST call or a redirect URL path substitution.
        button_text: The button label to display in the UI.
        page_names: The pages of the UI on which to display the button.
        redirect_url: The URL that the browser should load
        open_in_new_tab: If true, the specified URL opens in a new browser tab.
        output_dir: Directory to save yaml file
        button_name: Name of yaml file

    Returns:
    """

    # Minor url validation
    if not redirect_url.startswith(("http://", "https://")):
        value_error_message = f"Invalid url. Must begin with http:// or https://"
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    button_dict = {
        "buttonType": "redirectButton",
        "buttonId": button_id,
        "buttonText": button_text,
        "pageNames": page_names,
        "redirectUrl": redirect_url,
        "openInNewTab": open_in_new_tab,
    }

    file = f"{button_name}.yaml"
    filepath = os.path.join(output_dir, file)

    LOGGER.info(f"Saving {file} to {output_dir}")
    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(button_dict, yaml_file, sort_keys=False)


def create_post_button(
    *,
    button_id: str,
    button_text: str,
    page_names: List[str],
    post_url: str,
    post_body_keys: List[str],
    success_message: str,
    fail_message: str,
    display_response: bool,
    output_dir: str,
    button_name: str,
) -> dict:
    """Create yaml file with all required attributes for
    a 'POST' UI button

    Args:
        button_id: A short identifier for the button to use in the,
                   body of a POST call or a redirect URL path substitution.
        button_text: The button label to display in the UI.
        page_names: The pages of the UI on which to display the button.
        post_url: The target URL for a POST API call
        post_body_keys: Specifies the keys to request in the body of the POST call
        success_message: The message that displays to the user when the POST call succeeds.
        fail_message: The message that displays to the user when the POST call fails.
        display_response: Whether the contents of the API response body should display to the user.
        output_dir: Directory to save yaml file
        button_name: Name of yaml file

    Returns:
        Dictionary with required attribute names for a POST button
    """
    # Minor url validation
    if not post_url.startswith(("http://", "https://")):
        value_error_message = f"Invalid url. Must begin with http:// or https://"
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    button_dict = {
        "buttonType": "postButton",
        "buttonId": button_id,
        "buttonText": button_text,
        "pageNames": page_names,
        "postUrl": post_url,
        "postBodyKeys": post_body_keys,
        "successMessage": success_message,
        "failMessage": fail_message,
        "displayResponse": display_response,
    }

    file = f"{button_name}.yaml"
    filepath = os.path.join(output_dir, file)

    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(button_dict, yaml_file, sort_keys=False)


def register_buttons(
    *,
    buttons: List[str],
    tamr_install_dir: str,
    remote_client: Optional["paramiko.SSHClient"] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
):
    """Registers a list of buttons in a Tamr instance.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.

    Args:
        buttons: A list of yaml files (absolute paths) with button configs
        tamr_install_dir: Full path to directory where Tamr is installed
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
    """
    LOGGER.info(f"Registering the following buttons in Tamr: {buttons}")

    for button in buttons:
        # Run the register command
        command = (
            f"{tamr_install_dir}/tamr/utils/unify-admin.sh ui:config --extensionConfig {button}"
        )

        LOGGER.info(f"Registering {button}")
        _run_command(
            command=command,
            remote_client=remote_client,
            impersonation_username=impersonation_username,
            impersonation_password=impersonation_password,
            enforce_success=True,
        )


def register_button(
    *,
    button: str,
    tamr_install_dir: str,
    remote_client: Optional["paramiko.SSHClient"] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
):
    """Registers a button in a Tamr instance.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.

    Args:
        button: Path to a yaml file with button configs
        tamr_install_dir: Full path to directory where Tamr is installed
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
    """
    buttons = [button]

    register_buttons(
        buttons=buttons,
        tamr_install_dir=tamr_install_dir,
        remote_client=remote_client,
        impersonation_username=impersonation_username,
        impersonation_password=impersonation_password,
    )


def create_button_extension(extension_name: str, buttons: List[str], output_dir: str):
    """Given a list of button yaml files, save it as an extension yaml file

    Args:
        extension_name: Name of button extension to save
        buttons: List of button yaml files (absolute paths)
        output_dir: directory in which to save yaml extension file

    Returns:
    """
    # Create dicts from the yaml files
    dict_list = []
    for file in buttons:
        d = yaml.load(file)
        dict_list.append(d)

    file = f"{extension_name}.yaml"
    filepath = os.path.join(output_dir, file)

    output_dict = {"extensionName": extension_name, "buttons": buttons}

    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(output_dict, yaml_file, sort_keys=False)
