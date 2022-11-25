"Helper functions related to creating & managing custom UI buttons as yaml files"

import logging
from typing import List
import yaml

LOGGER = logging.getLogger(__name__)


def create_redirect_dict(
    *,
    button_id: str,
    button_text: str,
    page_names: List[str],
    redirect_url: str,
    open_in_new_tab: bool,
) -> dict:
    """Create python dictionary with all required attributes for
    a 'REDIRECT' UI button

    Args:
        button_id: A short identifier for the button to use in the body of a POST call or a redirect URL path substitution.
        button_text: The button label to display in the UI.
        page_names: The pages of the UI on which to display the button.
        redirect_url: The URL that the browser should load
        open_in_new_tab: If true, the specified URL opens in a new browser tab.

    Returns:
        Dictionary with required attribute names for REDIRECT button
    """

    button_dict = {
        "buttonType": "redirectButton",
        "buttonId": button_id,
        "buttonText": button_text,
        "pageNames": page_names,
        "redirectUrl": redirect_url,
        "openInNewTab": open_in_new_tab,
    }

    return button_dict


def create_post_dict(
    button_id: str,
    button_text: str,
    page_names: List[str],
    post_url: str,
    post_body_keys: List[str],
    success_message: str,
    fail_message: str,
    display_response: bool,
) -> dict:
    """Create python dictionary with all required attributes for
    a 'POST' UI button

    Args:
        button_id: A short identifier for the button to use in the body of a POST call or a redirect URL path substitution.
        button_text: The button label to display in the UI.
        page_names: The pages of the UI on which to display the button.
        post_url: The target URL for a POST API call
        post_body_keys: Specifies the keys to request in the body of the POST call
        success_message: The message that displays to the user when the POST call succeeds.
        fail_message: The message that displays to the user when the POST call fails.
        display_response: Whether the contents of the API response body should display to the user.

    Returns:
        Dictionary with required attribute names for a POST button
    """

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

    # button_yaml = yaml.dump(button_dict)
    return button_dict


def save_button_as_yaml(button_name: str, button_dict: dict, output_dir: str):
    """Given a button dictionary, save it as a yaml file

    Args:
        button_name: Name of button to save
        button_dict: Dict of button config values
        output_dir: directory in which to save yaml file

    Returns:
    """

    filepath = f"{output_dir}/{button_name}.yaml"

    with open(f"{filepath}", "w") as yaml_file1:
        yaml.dump(button_dict, yaml_file1, sort_keys=False)

    return


def create_button_extension(extension_name: str, buttons: List[dict], output_dir: str):
    """Given a list of button dictionaries, save it as an extension yaml file

    Args:
        extension_name: Name of button extension to save
        buttons: List of button dictionaries
        output_dir: directory in which to save yaml file

    Returns:
    """

    filepath = f"{output_dir}/{extension_name}.yaml"

    output_dict = {"extensionName": extension_name, "buttons": buttons}

    with open(f"{filepath}", "w") as yaml_file1:
        yaml.dump(output_dict, yaml_file1, sort_keys=False)

    return
