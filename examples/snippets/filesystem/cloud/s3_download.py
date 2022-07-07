"""
An example script to demonstrate the use of the s3_download function to download file,
from an S3 bucket.
"""

import tamr_toolbox as tbox
import boto3

s3_client = boto3.client("s3")

# download file from AWS S3
# download a file on GCS "s3://my-bucket/path-to-file" to "my_local_directory/my_file.txt"
tbox.filesystem.cloud.s3_download(
    cloud_client=s3_client,
    source_filepath="path-to-file",
    destination_filepath="my_local_directory/my_file.txt",
    bucket_name="my-bucket",
)
