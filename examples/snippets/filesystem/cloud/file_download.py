"""
An example script to demonstrate the use of the file_download function to download file from.
"""

import tamr_toolbox as tbox
from google.cloud.client import Client as GcsClient

gcs_client = GcsClient()

# example of download_file from passed client (gcs)
# download a file on GCS "gs://my-bucket/path-to-file" to "my_local_directory/my_file.txt"
tbox.filesystem.cloud.file_download(
    client_type="gcs",
    cloud_client=gcs_client,
    source_filepath="path-to-file",
    destination_filepath="my_local_directory/my_file.txt",
    bucket_name="my-bucket",
)
