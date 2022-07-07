"""
An example script to demonstrate the use of the s3_download function to download file from.
"""

import tamr_toolbox as tbox
from boto3.session import Session

S3Client = Session("credentials").client("s3")

# download file from AWS S3
# download a file on GCS "s3://my-bucket/path-to-file" to "my_local_directory/my_file.txt"
tbox.filesystem.cloud.s3_download(
    cloud_client=S3Client,
    source_filepath="path-to-file",
    destination_filepath="my_local_directory/my_file.txt",
    bucket_name="my-bucket",
)
