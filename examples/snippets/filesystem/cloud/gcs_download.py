"""
An example script to demonstrate the use of the gcs_download function to download file from.
"""

import tamr_toolbox as tbox
from google.cloud.client import Client as GcsClient

gcs_client = GcsClient()

# download file from gcs
# download a file on GCS "gs://my-bucket/path-to-file" to "my_local_directory/my_file.txt"
tbox.filesystem.cloud.gcs_download(
    cloud_client=gcs_client,
    source_filepath="path-to-file",
    destination_filepath="my_local_directory/my_file.txt",
    bucket_name="my-bucket",
)