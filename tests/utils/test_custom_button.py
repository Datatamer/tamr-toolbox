"""Tests for tasks related to loading and using configuration files"""
import os
from pathlib import Path
import pytest
from tamr_toolbox.utils.custom_button import *
from tests._common import get_toolbox_root_dir


def test_create_redirect_with_invalid_url():

    output_directory = get_toolbox_root_dir()
    # Expect failure when incorrectly formatted url provided
    with pytest.raises(ValueError):
        create_redirect_button(
            button_id="redirect_button_1",
            button_text="Google",
            page_names=["Schema Mapping:Dashboard", "Mastering:Dashboard"],
            redirect_url="www.google.com",
            open_in_new_tab=True,
            output_dir=output_directory,
            button_name="redirect_button_1",
        )


def test_create_post_with_invalid_pagenames():

    output_directory = get_toolbox_root_dir()
    # Expect failure when page name(s) provided are invalid
    with pytest.raises(ValueError):
        create_post_button(
            button_id="post_button_1",
            button_text="Export Project",
            page_names=["Schema Mapping:Clusters", "Home", "Random Page Name"],
            post_url="https://example-location.tamr.com/python/api/export",
            post_body_keys=["projectId"],
            success_message="Project export sucessful",
            fail_message="Project export failed",
            display_response=True,
            output_dir=output_directory,
            button_name="post_button_1",
        )
