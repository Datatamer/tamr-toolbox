"""Snippet for importing project artifacts into a Tamr project"""
import tamr_toolbox as tbox
from tamr_toolbox.project import categorization

# from tamr_toolbox.models.project_artifacts import CategorizationArtifacts as catfacts

# Read config, make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Get project object (only necessary if importing into existing project)
project = tamr.projects.by_name('current_categorization_project')

# Set path to project artifact zip (on server containing tamr instance)
artifact_path = "/home/ubuntu/tamr/projectExports/minimal_categorization-1622067179477.zip"

# Import project artifacts into existing project
# (overwrite_existing flag is necessary for this operation)
op_1 = categorization.import_artifacts(project_artifact_path=str(artifact_path),
                                       tamr_client=tamr,
                                       target_project=project,
                                       overwrite_existing=True
                                       )

# Print operation
print(op_1)

# Import project artifacts into new project
op_2 = categorization.import_artifacts(project_artifact_path=str(artifact_path),
                                       tamr_client=tamr,
                                       new_project_name='new_categorization'
                                       )

# Print operation
print(op_2)
