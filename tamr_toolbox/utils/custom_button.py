"""
Helper functions related to creating & managing custom UI buttons as yaml files.

Important: Custom buttons are only available to versions 2022.008.0 and later
"""
import logging
from typing import List, Optional, Union
import yaml
import os

from tamr_unify_client import Client
from tamr_toolbox.sysadmin.instance import _run_command
from tamr_toolbox.utils.version import is_version_condition_met, current

LOGGER = logging.getLogger(__name__)

TAMR_RELEASE_VERSION = "2022.008.0"


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


def _check_valid_abs_path(dir: str):
    """
    Function to check provided path is absolute path
    and is not $TAMR_HOME/tamr/auxiliary-sevrices/conf

    Args:
        dir: directory provided

    Returns:

    Raises:
        ValueError: If path is not absolute or equal to
                    $TAMR_HOME/tamr/auxiliary-sevrices/conf
    """
    # Check absolute
    if not os.path.isabs(dir):
        value_error_message = f"Output directory must be absolute path."
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    bad_paths = ("tamr/auxiliary-sevrices/conf", "tamr/auxiliary-sevrices/conf/")

    if dir.endswith(bad_paths):
        value_error_message = (
            f"Output directory must not be $TAMR_HOME/tamr/auxiliary-sevrices/conf."
        )
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)


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
) -> str:
    """Create yaml file with all required attributes for
    a 'REDIRECT' UI button.
    Button features are only available to versions 2022.008.0 and later.

    Args:
        extension_name: Name of button extension
        button_id: A short identifier for the button to use in the,
                   body of a POST call or a redirect URL path substitution.
        button_text: The button label to display in the UI.
        page_names: The pages of the UI on which to display the button.
        redirect_url: The URL that the browser should load
        open_in_new_tab: If true, the specified URL opens in a new browser tab.
        output_dir: Directory to save yaml file (absolute path)
        button_name: Name of yaml file

    Returns:
        Path to yaml file created
    """
    # Path validation
    _check_valid_abs_path(output_dir)

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

    return filepath


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
) -> str:
    """
    Create yaml file with all required attributes for
    a 'POST' UI button.
    Button features are only available to versions 2022.008.0 and later.

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
        output_dir: Directory to save yaml file (absolute path)
        button_name: Name of yaml file

    Returns:
        Path to yaml file created
    """
    # Path validation
    _check_valid_abs_path(output_dir)

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

    LOGGER.info(f"Saving {file} to {output_dir}")
    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(button_dict, yaml_file, sort_keys=False)

    return filepath


def create_button_extension(*, extension_name: str, buttons: List[str], output_dir: str) -> str:
    """
    Given a list of button yaml files, save it as a grouped extension yaml file.
    Button features are only available to versions 2022.008.0 and later.

    Args:
        extension_name: Name of button extension to save
        buttons: List of button yaml files (absolute paths)
        output_dir: directory in which to save yaml extension file (absolute path)

    Returns:
        Path to yaml file created
    """
    # Path validation
    _check_valid_abs_path(output_dir)

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

    LOGGER.info(f"Saving {file} to {output_dir}")
    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(output_dict, yaml_file, sort_keys=False)

    return filepath


def create_button_extension_from_list(
    *, extension_name: str, output_dir: str, buttons: List[dict]
) -> str:
    """
    Given a list of button dictionaries, save it as a grouped extension yaml file
    Button features are only available to versions 2022.008.0 and later.

    Args:
        extension_name: Name of button extension to save
        output_dir: directory in which to save yaml extension file (absolute path)
        buttons: List of button dictionaries. Either redirect or post.
        Format examples:
        ---
        redirect:
        {
            "buttonType": "redirectButton",
            "buttonId": button_id,
            "buttonText": button_text,
            "pageNames": page_names,
            "redirectUrl": redirect_url,
            "openInNewTab": open_in_new_tab
        }
        ---
        post:
        {
            "buttonType": "postButton",
            "buttonId": button_id,
            "buttonText": button_text,
            "pageNames": page_names,
            "postUrl": post_url,
            "postBodyKeys": post_body_keys,
            "successMessage": success_message,
            "failMessage": fail_message,
            "displayResponse": display_response
        }
        ---

    Returns:
        Path to yaml file created
    """
    # Path Validation
    _check_valid_abs_path(output_dir)

    # URL & pagename validation
    invalid_urls = []
    invalid_pages = []
    for button_dict in buttons:
        if button_dict["buttonType"] == "postButton":
            url = button_dict["postUrl"]
        else:
            url = button_dict["redirectUrl"]

        if not url.startswith(("http://", "https://")):
            invalid_urls.append(url)

        invalid_pages += [
            p for p in button_dict["pageNames"] if not _check_valid_page_name(pagename=p)
        ]

    if len(invalid_urls) > 0 and len(invalid_pages) > 0:
        value_error_message = f"Invalid url and pagenames. \
            url(s) {invalid_urls} must begin with http:// or https:// \
            invalid page name(s): {invalid_pages}"
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)
    elif len(invalid_urls) > 0 and len(invalid_pages) == 0:
        value_error_message = f"Invalid url(s) {invalid_urls}. Must begin with http:// or https://"
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)
    elif len(invalid_urls) == 0 and len(invalid_pages) > 0:
        value_error_message = (
            f"Invalid pagename(s): {invalid_pages}. See docs for allowed Tamr button page names"
        )
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    file = f"{extension_name}.yaml"
    filepath = os.path.join(output_dir, file)

    output_dict = {"extensionName": extension_name, "buttons": buttons}

    LOGGER.info(f"Saving {file} to {output_dir}")
    with open(f"{filepath}", "w") as yaml_file:
        yaml.dump(output_dict, yaml_file, sort_keys=False)

    return filepath


def register_buttons(
    *,
    tamr_client: Client,
    buttons: Union[str, List[str]],
    tamr_install_dir: str,
    remote_client: Optional["paramiko.SSHClient"] = None,
    impersonation_username: Optional[str] = None,
    impersonation_password: Optional[str] = None,
):
    """
    Registers a list of button(s) in a Tamr instance.
    Requires Tamr restart to display buttons in UI.

    Runs in a remote environment if an ssh client is specified otherwise runs in the local shell.
    If an impersonation_username is provided, the command is run as the provided user.
    If an impersonation_password is provided, password authentication is used for impersonation,
    otherwise sudo is used.
    Button features are only available to versions 2022.008.0 and later.

    Version:
        Requires Tamr 2022.008.0 or later

    Args:
        tamr_client: Tamr Client object
        buttons: An individual string or a list of yaml files (absolute paths) with button configs
        tamr_install_dir: Full path to directory where Tamr is installed
        remote_client: An ssh client providing a remote connection
        impersonation_username: A bash user to run the command as,
            this should be the tamr install user
        impersonation_password: The password for the impersonation_username

    Returns:
    """
    # Tamr version check
    minimum_tamr_version = TAMR_RELEASE_VERSION
    tamr_version = current(tamr_client)

    is_version_condition_met(
        tamr_version=tamr_version, min_version=minimum_tamr_version, raise_error=True
    )

    if isinstance(buttons, str):
        buttons = [buttons]

    # Clean up path
    _check_valid_abs_path(tamr_install_dir)

    if tamr_install_dir.endswith("/"):
        tamr_install_dir = tamr_install_dir[:-1]

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


def delete_buttons(*, button_files: Union[str, List[str]], tamr_install_dir: str):
    """Given a list of button yaml files, delete them thus removing the button from UI.

       NB: Registered buttons are located in $TAMR_HOME/tamr/auxiliary-sevrices/conf
           Requires restart of Tamr to register deletion.
           Button features are only available to versions 2022.008.0 and later.

    Args:
        button_files: Individual string or list of button yaml files (absolute paths)
        tamr_install_dir: Full path to directory where Tamr is installed (absolute path)
    Returns:
    """
    if isinstance(button_files, str):
        button_files = [button_files]

    # Check all files exist
    missing_files = [f for f in button_files if not os.path.exists(f)]
    if len(missing_files) > 0:
        warning_message = f"File(s) {missing_files} not found"
        LOGGER.warning(warning_message)

    # Work with present files only
    present_files = [x for x in button_files if x not in set(missing_files)]

    button_dir = os.path.join(tamr_install_dir, "tamr/auxiliary-services/conf")

    # Check present button files provided reside in correct folder.
    path_list = present_files + [button_dir]

    if os.path.commonpath(path_list) != button_dir:
        value_error_message = f"All button files provided must belong to {button_dir} \
             otherwise deletion will not register."
        LOGGER.error(value_error_message)
        raise ValueError(value_error_message)

    LOGGER.info(f"Removing yaml files from {button_dir}")
    for file in present_files:
        os.remove(file)
        LOGGER.info(f"Deleted {file}")
