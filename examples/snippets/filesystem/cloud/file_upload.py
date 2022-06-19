"""
An example script to demonstrate the use of the file_upload function to upload file to.
"""

import tamr_toolbox as tbox
from google.cloud.client import Client as GcsClient

gcs_client = GcsClient()

# uploads file to passed client (gcs)
# upload a local file "my_local_directory/my_file.txt" to "gs://my-bucket/path-to-file"
tbox.filesystem.cloud.file_upload(
    client_type="gcs",
    cloud_client=gcs_client,
    source_filepath="my_local_directory/my_file.txt",
    destination_filepath="path-to-file",
    bucket_name="my-bucket",
)