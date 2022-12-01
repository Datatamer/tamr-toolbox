"Helper functions related to creating & managing custom UI buttons as yaml files"

import logging
from typing import List, Optional
import yaml
import os

from tamr_toolbox.sysadmin.instance import _run_command

LOGGER = logging.getLogger(__name__)


def _check_valid_page_name(*, pagename: str):
    """Checks that pagename provided is a valid Tamr button page name
    Args:
        pagename: Name of page provided

    Returns:
        True if page name is acceptable
    """
    valid_pagenames = [
        "Dataset Catalog",
        "Home",
        "Jobs",
        "Policies",
        "Users and Groups",
        "Categorization:Categorizations",
        "Categorization:Category Details",
        "Categorization:Dashboard",
        "Categorization:Project Datasets",
        "Categorization:Schema Mapping",
        "Categorization:Taxonomy",
        "Categorization:Unified Dataset",
        "Enrichment:Enrichment",
        "Golden Records:Golden Records",
        "Golden Records:Rules",
        "Mastering:Binning",
        "Mastering:Clusters",
        "Mastering:Dashboard",
        "Mastering:Group Records",
        "Mastering:Pairs",
        "Mastering:Project Datasets",
        "Mastering:Schema Mapping",
        "Mastering:Unified Dataset",
        "Schema Mapping:Dashboard",
        "Schema Mapping:Project Datasets",
        "Schema Mapping:Schema Mapping",
        "Schema Mapping:Unified Dataset",
    ]

    if pagename not in valid_pagenames:
        return False
    else:
        return True


def create_redirect_button(
    *,
    extension_name: str,
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
        extension_name: Name of button extension
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

    # Page name validation
    invalid_pages = [p for p in page_names if not _check_valid_page_name(pagename=p)]
    if len(invalid_pages) > 0:
        value_error_message = (
            f"Invalid pagename(s): {invalid_pages}. See docs for allowed Tamr button page names"
        )
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    button_dict = {
        "extensionName": extension_name,
        "buttons": [
            {
                "buttonType": "redirectButton",
                "buttonId": button_id,
                "buttonText": button_text,
                "pageNames": page_names,
                "redirectUrl": redirect_url,
                "openInNewTab": open_in_new_tab,
            }
        ],
    }

    file = f"{button_name}.yaml"
    filepath = os.path.join(output_dir, file)

    LOGGER.info(f"Saving {file} to {output_dir}")
    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(button_dict, yaml_file, sort_keys=False)


def create_post_button(
    *,
    extension_name: str,
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
        extension_name: Name of button extension
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
    """
    # Minor url validation
    if not post_url.startswith(("http://", "https://")):
        value_error_message = f"Invalid url. Must begin with http:// or https://"
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    # Page name validation
    invalid_pages = [p for p in page_names if not _check_valid_page_name(pagename=p)]
    if len(invalid_pages) > 0:
        value_error_message = (
            f"Invalid pagename(s): {invalid_pages}. See docs for allowed Tamr button page names"
        )
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    button_dict = {
        "extensionName": extension_name,
        "buttons": [
            {
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
        ],
    }

    file = f"{button_name}.yaml"
    filepath = os.path.join(output_dir, file)

    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(button_dict, yaml_file, sort_keys=False)


def create_button_extension(*, extension_name: str, buttons: List[str], output_dir: str):
    """Given a list of button yaml files, save it as a grouped extension yaml file

    Args:
        extension_name: Name of button extension to save
        buttons: List of button yaml files (absolute paths)
        output_dir: directory in which to save yaml extension file

    Returns:
    """
    # Create dicts from the yaml files
    dict_list = []
    for file in buttons:
        with open(file, "r") as loaded_file:
            button_object = yaml.safe_load(loaded_file)
            button_dict = button_object["buttons"][0]
            dict_list.append(button_dict)

    file = f"{extension_name}.yaml"
    filepath = os.path.join(output_dir, file)

    output_dict = {"extensionName": extension_name, "buttons": dict_list}

    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(output_dict, yaml_file, sort_keys=False)


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


def delete_buttons(*, button_files: List[str], tamr_install_dir: str):
    """Given a list of button yaml files, delete them thus removing the button from UI.

       NB: Registered buttons are located in $TAMR_HOME/tamr/auxiliary-sevrices/conf
           Requires restart of Tamr to register deletion.

    Args:
        button_files: List of button yaml files (absolute paths)
        tamr_install_dir: Full path to directory where Tamr is installed
    Returns:
    """
    # Check all files exist
    missing_files = [f for f in button_files if not os.path.exists(f)]
    if len(missing_files) > 0:
        error_message = f"File(s) {missing_files} not found"
        LOGGER.error(error_message)
        raise FileNotFoundError(error_message)

    button_dir = os.path.join(tamr_install_dir, "tamr/auxiliary-services/conf")

    # Check button files provided reside in correct folder.
    path_list = button_files + [button_dir]

    if os.path.commonpath(path_list) != button_dir:
        value_error_message = f"All button files provided must belong to {button_dir} otherwise deletion will not register."
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    LOGGER.info(f"Removing yaml files from {button_dir}")
    for file in button_files:
        LOGGER.debug(f"Deleting {file}")
        os.remove(file)
