"""Tests for tasks related to creation of Slack notifications"""
import pytest
from pymsteams import connectorcard

from requests import HTTPError
from unittest.mock import MagicMock
from tamr_toolbox import utils, notifications
from tamr_toolbox.utils.operation import from_resource_id, get_details
from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir


CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/notifications.config.yaml"
)


client = utils.client.create(**CONFIG["my_instance_name"])
project = client.projects.by_resource_id(CONFIG["projects"]["minimal_schema_mapping"])
op = project.unified_dataset().refresh(asynchronous=True)

# list_responses = notifications.teams.monitor_job(
#     tamr=client,
#     teams_connector_card=connectorcard("https://httpstat.us/200"),
#     operation=op,
#     poll_interval_seconds=0.01,
# )