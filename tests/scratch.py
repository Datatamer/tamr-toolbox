# from tests._common import get_toolbox_root_dir
from tamr_toolbox import utils

from tamr_toolbox.utils._project_name import _get_original_project_name

CONFIG = utils.config.from_yaml(
   "./tests/mocking/resources/toolbox_test.yaml"
)

client = utils.client.create(**CONFIG["toolbox_test_instance"])

print(_get_original_project_name(client, project_id=6))
