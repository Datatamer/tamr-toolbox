"""Snippet for exporting project artifacts from a Tamr project"""
import tamr_toolbox as tbox
from tamr_toolbox.project import export_artifacts
from tamr_toolbox.models.project_artifacts import CategorizationArtifacts

# Read config, make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Get project object
project = tamr.projects.by_resource_id("my_project_id")

# Set path to export directory (on server containing tamr instance)
path_export_dir = "/home/ubuntu/tamr/projectExports"

# Make list of categorization artifacts to exclude.
# You can spell out the artifact code if known,
# or list access via the CategorizationArtifacts dataclass
exclude_list = [
    CategorizationArtifacts.CATEGORIZATION_VERIFIED_LABELS,
    "CATEGORIZATION_TAXONOMIES",
    CategorizationArtifacts.CATEGORIZATION_FEEDBACK,
]

# Export project artifacts
op = export_artifacts(
    project=project,
    artifact_directory_path=path_export_dir,
    exclude_artifacts=exclude_list,
    asynchronous=False,
)

# Print operation
print(op)
