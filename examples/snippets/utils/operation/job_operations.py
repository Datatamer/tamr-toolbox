"""Snippet for interacting with Tamr operations (or jobs)"""
import tamr_toolbox as tbox

# Make Tamr Client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Create an unresolved operation, i.e., PENDING or RUNNING
project = tamr.projects.by_name("my_project_id")
op = project.unified_dataset().refresh(asynchronous=True)

# Monitor the operation
op_update = tbox.utils.operation.monitor(operation=op)

# Wait for the operation to finish
op_finished = tbox.utils.operation.wait(operation=op)

# Get the most recent operation
latest_op = tbox.utils.operation.get_latest(tamr=tamr)

# Get all operations
all_ops = tbox.utils.operation.get_all(tamr=tamr)

# Get active operations
active_ops = tbox.utils.operation.get_active(tamr=tamr)

# Create an operation from a job id
op = tbox.utils.operation.from_resource_id(tamr=tamr, job_id=op.resource_id)

# Get the details of an operation
text_details = tbox.utils.operation.get_details(operation=op)
